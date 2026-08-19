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

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkEnv
import org.apache.spark.sql.connect.config.Connect

/**
 * A session-scoped set of environment variables to install in the environment of Python worker
 * processes that Spark launches for this session's Python functions.
 *
 * Values may carry credentials, so a subset of the keys can be marked sensitive. The marking
 * controls only how this object is rendered for logs and diagnostics; it never changes what a
 * Python worker observes. Callers that do not know which keys are sensitive should treat all of
 * them as sensitive, which is what [[PythonWorkerEnvironment.of]] does when `sensitiveKeys` is
 * empty but `variables` is not.
 *
 * Instances are immutable. Replacing a session's environment swaps in a whole new instance rather
 * than mutating one in place, so a concurrent reader always observes a complete environment.
 *
 * @param variables
 *   the environment variables, keyed by name. Names are case-sensitive, matching POSIX.
 * @param sensitiveKeys
 *   the subset of `variables` keys whose values must not be logged. Matched by exact,
 *   case-sensitive equality; a name that is not a key of `variables` is ignored.
 */
private[connect] case class PythonWorkerEnvironment(
    variables: Map[String, String],
    sensitiveKeys: Set[String]) {

  def isEmpty: Boolean = variables.isEmpty

  def nonEmpty: Boolean = variables.nonEmpty

  /** Whether the value of `key` must be withheld from logs and diagnostics. */
  def isSensitive(key: String): Boolean = sensitiveKeys.contains(key)

  /**
   * A fresh mutable copy for a single Python function.
   *
   * A copy is required rather than a shared map: the Python runners add their own entries to the
   * map they are given before launching a worker, so sharing one map across functions would leak
   * those entries between functions, and an immutable map would fail the same assignment.
   */
  def toMutableJavaMap: java.util.HashMap[String, String] = {
    val result = new java.util.HashMap[String, String](variables.size)
    variables.foreach { case (key, value) => result.put(key, value) }
    result
  }

  /**
   * A representation for logs and error messages. Values of sensitive keys are replaced; every
   * key name and every non-sensitive value is kept, so that ordinary configuration stays
   * debuggable.
   */
  def redactedString: String = {
    variables.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        if (isSensitive(key)) s"$key=${PythonWorkerEnvironment.redactedValue}" else s"$key=$value"
      }
      .mkString("{", ", ", "}")
  }
}

private[connect] object PythonWorkerEnvironment {

  val empty: PythonWorkerEnvironment = PythonWorkerEnvironment(Map.empty, Set.empty)

  /** Placeholder substituted for the value of a sensitive key. */
  val redactedValue: String = "[REDACTED]"

  /**
   * Environment variable names accepted by this layer. Deliberately the same shape enforced by
   * callers further upstream, so that this layer does not reject a name they already accepted.
   */
  val keyPattern: String = "^[A-Za-z_][A-Za-z0-9_]*$"

  private val compiledKeyPattern = keyPattern.r

  // Keys are not secret -- audit trails record them -- but a rejected name can be arbitrarily
  // long, so messages carry a bounded prefix instead of the whole name.
  private val maxKeyCharsInMessage = 32

  private def abbreviate(key: String): String = {
    if (key.length <= maxKeyCharsInMessage) key
    else s"${key.take(maxKeyCharsInMessage)}..."
  }

  /**
   * Validates `variables` and `sensitiveKeys` and builds an environment from them.
   *
   * Validation is all-or-nothing: a rejected input throws and produces no environment, so a
   * caller that keeps the previous environment on failure never installs a partially applied one.
   *
   * These are internal precondition checks. Nothing user-facing reaches this method yet, so a
   * rejection is a caller error rather than something a user can provoke, and it is reported as
   * such. Turning these into user-facing error conditions belongs with the public API that first
   * makes them reachable, when their wording can be reviewed as user-visible text.
   *
   * A message may name a variable but never carries its value, and a name is truncated rather
   * than echoed whole, so that a rejection cannot leak a credential into a log or a stack trace.
   *
   * When `variables` is non-empty and `sensitiveKeys` is empty, every key is marked sensitive. An
   * absent marking means "not classified", and withholding values is the safe reading of that.
   *
   * @throws IllegalArgumentException
   *   if a name is malformed or too long, a value is null, or the collection exceeds a limit.
   */
  def of(variables: Map[String, String], sensitiveKeys: Set[String]): PythonWorkerEnvironment = {
    val conf = SparkEnv.get.conf
    val maxCount = conf.get(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_VARIABLES)
    val maxKeyLength = conf.get(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_KEY_LENGTH)
    val maxTotalSizeBytes = conf.get(Connect.CONNECT_PYTHON_WORKER_ENV_MAX_TOTAL_SIZE_BYTES)

    require(
      variables.size <= maxCount,
      s"Cannot set ${variables.size} environment variables for Python workers, " +
        s"which is more than the maximum of $maxCount.")

    var totalSizeBytes = 0L
    variables.foreach { case (key, value) =>
      require(
        key.length <= maxKeyLength,
        s"The environment variable name '${abbreviate(key)}' has ${key.length} characters, " +
          s"which is more than the maximum of $maxKeyLength.")
      // `matches` requires the whole name to match. Searching for the pattern instead would accept
      // a name with a trailing newline, because `$` also matches before a terminating line break.
      require(
        compiledKeyPattern.matches(key),
        s"'${abbreviate(key)}' is not a valid environment variable name. " +
          s"Names must match $keyPattern.")
      // An empty value is allowed, matching `FOO=` in a POSIX shell. A null value is not: it is
      // indistinguishable from "unset" once installed, so the caller has to say which it meant.
      require(
        value != null,
        s"The environment variable '${abbreviate(key)}' has a null value. " +
          "Use an empty string to set an empty value.")
      totalSizeBytes += utf8Length(key) + utf8Length(value)
    }

    require(
      totalSizeBytes <= maxTotalSizeBytes,
      s"The environment variables for Python workers total $totalSizeBytes bytes, " +
        s"which is more than the maximum of $maxTotalSizeBytes.")

    val effectiveSensitiveKeys = if (variables.nonEmpty && sensitiveKeys.isEmpty) {
      variables.keySet
    } else {
      sensitiveKeys.intersect(variables.keySet)
    }
    PythonWorkerEnvironment(variables, effectiveSensitiveKeys)
  }

  /**
   * The names in `sensitiveKeys` that do not name a variable in `variables`.
   *
   * Such a name is ignored rather than rejected, but it means the marking and the variables
   * disagree, so callers are expected to surface it.
   */
  def unmatchedSensitiveKeys(
      variables: Map[String, String],
      sensitiveKeys: Set[String]): Set[String] = sensitiveKeys.diff(variables.keySet)

  private def utf8Length(s: String): Long = s.getBytes(StandardCharsets.UTF_8).length.toLong
}
