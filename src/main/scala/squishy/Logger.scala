/* Logger.scala
 * 
 * Copyright (c) 2013 bizo.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * This file is largely derived from https://github.com/aboisvert/pixii.
 */
package squishy

/**
 * A plugable warning and error logger.
 */
trait Logger {

  /** Logs a waning message. */
  def warn(message: String): Unit

  /** Logs an error message and exception. */
  def error(message: String, t: Throwable): Unit

}

/**
 * Common logger implementations.
 */
object Logger {

  /**
   * A logger that prints to the console, useful for development.
   */
  trait Console extends Logger {

    /** @inheritdoc */
    override def warn(message: String) {
      Console.println("[WARN]  " + message)
    }

    /** @inheritdoc */
    override def error(message: String, t: Throwable) {
      Console.println("[ERROR] " + message)
      t.printStackTrace()
    }

  }

}