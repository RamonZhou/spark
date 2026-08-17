/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect.service

import java.util.UUID

import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class PythonWorkerEnvironmentSuite extends SharedSparkSession {

  private def newHolder(): SessionHolder =
    SparkConnectTestUtils.createDummySessionHolder(spark)

  // ---------------------------------------------------------------------------
  // PythonWorkerEnvironment: construction and validation
  // ---------------------------------------------------------------------------

  test("SPARK-58752: empty environment has no variables") {
    assert(PythonWorkerEnvironment.empty.isEmpty)
    assert(!PythonWorkerEnvironment.empty.nonEmpty)
    assert(PythonWorkerEnvironment.empty.variables.isEmpty)
    assert(PythonWorkerEnvironment.empty.sensitiveKeys.isEmpty)
  }

  test("SPARK-58752: valid variables are stored as given") {
    val env = PythonWorkerEnvironment.of(Map("FOO" -> "1", "BAR" -> ""), Set("FOO"))
    assert(env.variables === Map("FOO" -> "1", "BAR" -> ""))
    assert(env.sensitiveKeys === Set("FOO"))
    assert(env.isSensitive("FOO"))
    assert(!env.isSensitive("BAR"))
  }

  test("SPARK-58752: an empty value is allowed") {
    val env = PythonWorkerEnvironment.of(Map("FOO" -> ""), Set("FOO"))
    assert(env.variables("FOO") === "")
  }

  test("SPARK-58752: no classification means every value is treated as sensitive") {
    val env = PythonWorkerEnvironment.of(Map("FOO" -> "1", "BAR" -> "2"), Set.empty)
    assert(env.sensitiveKeys === Set("FOO", "BAR"))
  }

  test("SPARK-58752: sensitive names that are not variables are dropped") {
    val env = PythonWorkerEnvironment.of(Map("FOO" -> "1"), Set("FOO", "NOT_SET"))
    assert(env.sensitiveKeys === Set("FOO"))
  }

  test("SPARK-58752: unmatchedSensitiveKeys reports names that are not variables") {
    val unmatched =
      PythonWorkerEnvironment.unmatchedSensitiveKeys(Map("FOO" -> "1"), Set("FOO", "NOT_SET"))
    assert(unmatched === Set("NOT_SET"))
  }

  test("SPARK-58752: variable names are case-sensitive") {
    val env = PythonWorkerEnvironment.of(Map("FOO" -> "upper", "foo" -> "lower"), Set("FOO"))
    assert(env.variables("FOO") === "upper")
    assert(env.variables("foo") === "lower")
    // Marking one case does not mark the other.
    assert(env.isSensitive("FOO"))
    assert(!env.isSensitive("foo"))
  }

  test("SPARK-58752: a malformed name is rejected") {
    // "FOO\n" is included because an anchored pattern that is searched for rather than matched
    // against the whole name would accept a trailing newline.
    Seq("1FOO", "FOO-BAR", "FOO BAR", "FOO=BAR", "", "FOO.BAR", "FOO\n", "\nFOO").foreach { key =>
      val ex = intercept[IllegalArgumentException] {
        PythonWorkerEnvironment.of(Map(key -> "1"), Set.empty)
      }
      assert(
        ex.getMessage.contains("is not a valid environment variable name"),
        s"unexpected message for key '$key'")
    }
  }

  test("SPARK-58752: a name over the length limit is rejected") {
    val longKey = "A" * 513
    val ex = intercept[IllegalArgumentException] {
      PythonWorkerEnvironment.of(Map(longKey -> "1"), Set.empty)
    }
    assert(ex.getMessage.contains("more than the maximum"))
    // The name is bounded in the message rather than echoed whole.
    assert(!ex.getMessage.contains(longKey))
    assert(ex.getMessage.contains("513"))
  }

  test("SPARK-58752: a null value is rejected") {
    val ex = intercept[IllegalArgumentException] {
      PythonWorkerEnvironment.of(Map("FOO" -> null), Set.empty)
    }
    assert(ex.getMessage.contains("has a null value"))
    assert(ex.getMessage.contains("FOO"))
  }

  test("SPARK-58752: more variables than the limit is rejected") {
    val tooMany = (0 until 101).map(i => s"VAR_$i" -> "1").toMap
    val ex = intercept[IllegalArgumentException] {
      PythonWorkerEnvironment.of(tooMany, Set.empty)
    }
    assert(ex.getMessage.contains("more than the maximum of 100"))
    assert(ex.getMessage.contains("101"))
    assert(ex.getMessage.contains("100"))
  }

  test("SPARK-58752: exactly the variable limit is accepted") {
    val atLimit = (0 until 100).map(i => s"VAR_$i" -> "1").toMap
    assert(PythonWorkerEnvironment.of(atLimit, Set.empty).variables.size === 100)
  }

  test("SPARK-58752: a total size over the limit is rejected") {
    // Two values that together exceed 128 KiB, while each name stays within its own limit.
    val half = "x" * (65 * 1024)
    val ex = intercept[IllegalArgumentException] {
      PythonWorkerEnvironment.of(Map("A" -> half, "B" -> half), Set.empty)
    }
    assert(ex.getMessage.contains("bytes, which is more than the maximum"))
    // Neither the names nor the values are echoed into the message.
    assert(!ex.getMessage.contains(half))
  }

  test("SPARK-58752: total size counts UTF-8 bytes rather than characters") {
    // A character that needs 3 bytes in UTF-8, built from its code point so that this file stays
    // ASCII. Repeated enough to pass the byte limit while the character count stays under it, so
    // the test fails if the limit is ever applied to characters instead of bytes.
    val threeByteChar = 0x4e2d.toChar.toString
    val value = threeByteChar * (50 * 1024)
    assert(value.length < 128 * 1024, "character count must stay under the limit")
    val ex = intercept[IllegalArgumentException] {
      PythonWorkerEnvironment.of(Map("A" -> value), Set.empty)
    }
    assert(ex.getMessage.contains("bytes, which is more than the maximum"))
  }

  // ---------------------------------------------------------------------------
  // PythonWorkerEnvironment: rendering and copying
  // ---------------------------------------------------------------------------

  test("SPARK-58752: redactedString withholds sensitive values and keeps the rest") {
    val env = PythonWorkerEnvironment.of(
      Map("SECRET_TOKEN" -> "s3cr3t", "LOG_LEVEL" -> "DEBUG"),
      Set("SECRET_TOKEN"))
    val rendered = env.redactedString
    assert(!rendered.contains("s3cr3t"))
    assert(rendered.contains(PythonWorkerEnvironment.redactedValue))
    // Names are not secret, and a non-sensitive value stays readable.
    assert(rendered.contains("SECRET_TOKEN"))
    assert(rendered.contains("LOG_LEVEL=DEBUG"))
  }

  test("SPARK-58752: toMutableJavaMap returns an independent mutable copy") {
    val env = PythonWorkerEnvironment.of(Map("FOO" -> "1"), Set.empty)
    val first = env.toMutableJavaMap
    first.put("ADDED_BY_RUNNER", "2")
    val second = env.toMutableJavaMap
    // Entries added to one copy do not reach the environment or a later copy.
    assert(!second.containsKey("ADDED_BY_RUNNER"))
    assert(env.variables.keySet === Set("FOO"))
    assert(second.get("FOO") === "1")
  }

  // ---------------------------------------------------------------------------
  // SessionHolder: setting, replacing and clearing
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a session starts with an empty environment") {
    assert(newHolder().pythonWorkerEnvironment.isEmpty)
  }

  test("SPARK-58752: setting an environment makes it readable on the session") {
    val holder = newHolder()
    holder.setPythonWorkerEnvironment(Map("FOO" -> "1"), Set("FOO"))
    assert(holder.pythonWorkerEnvironment.variables === Map("FOO" -> "1"))
    assert(holder.pythonWorkerEnvironment.isSensitive("FOO"))
  }

  test("SPARK-58752: setting replaces the whole environment rather than merging") {
    val holder = newHolder()
    holder.setPythonWorkerEnvironment(Map("FIRST" -> "1"), Set.empty)
    holder.setPythonWorkerEnvironment(Map("SECOND" -> "2"), Set.empty)
    assert(holder.pythonWorkerEnvironment.variables === Map("SECOND" -> "2"))
  }

  test("SPARK-58752: setting an empty environment clears it") {
    val holder = newHolder()
    holder.setPythonWorkerEnvironment(Map("FOO" -> "1"), Set.empty)
    holder.setPythonWorkerEnvironment(Map.empty, Set.empty)
    assert(holder.pythonWorkerEnvironment.isEmpty)
  }

  test("SPARK-58752: a rejected environment leaves the previous one in place") {
    val holder = newHolder()
    holder.setPythonWorkerEnvironment(Map("KEEP" -> "1"), Set.empty)
    intercept[IllegalArgumentException] {
      holder.setPythonWorkerEnvironment(Map("not a valid name" -> "2"), Set.empty)
    }
    assert(holder.pythonWorkerEnvironment.variables === Map("KEEP" -> "1"))
  }

  // ---------------------------------------------------------------------------
  // Cloning
  // ---------------------------------------------------------------------------

  test("SPARK-58752: a cloned session inherits the environment of its source") {
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectService.sessionManager.initializeBaseSession(() => spark.newSession())

    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val source = SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    source.setPythonWorkerEnvironment(Map("FOO" -> "1", "BAR" -> "2"), Set("FOO"))

    val cloned = SparkConnectService.sessionManager
      .cloneSession(sourceKey, UUID.randomUUID.toString, None)

    assert(cloned.pythonWorkerEnvironment.variables === Map("FOO" -> "1", "BAR" -> "2"))
    // The classification travels with the variables, so the clone redacts the same names.
    assert(cloned.pythonWorkerEnvironment.isSensitive("FOO"))
    assert(!cloned.pythonWorkerEnvironment.isSensitive("BAR"))
  }

  test("SPARK-58752: a cloned session of a source with no environment has none") {
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectService.sessionManager.initializeBaseSession(() => spark.newSession())

    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)

    val cloned = SparkConnectService.sessionManager
      .cloneSession(sourceKey, UUID.randomUUID.toString, None)

    assert(cloned.pythonWorkerEnvironment.isEmpty)
  }

  test("SPARK-58752: changing the source environment after cloning does not change the clone") {
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectService.sessionManager.initializeBaseSession(() => spark.newSession())

    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val source = SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    source.setPythonWorkerEnvironment(Map("FOO" -> "1"), Set.empty)

    val cloned = SparkConnectService.sessionManager
      .cloneSession(sourceKey, UUID.randomUUID.toString, None)
    source.setPythonWorkerEnvironment(Map("FOO" -> "changed"), Set.empty)

    assert(cloned.pythonWorkerEnvironment.variables === Map("FOO" -> "1"))
  }
}
