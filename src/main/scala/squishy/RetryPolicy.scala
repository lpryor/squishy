/* RetryPolicy.scala
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

import com.amazonaws.AmazonServiceException
import java.util.concurrent.{ Executors, TimeUnit }

/**
 * A plugagable retry policy for dealing with Amazon SQS errors.
 */
trait RetryPolicy {

  /** Executes a synchronous operation, handling errors in accordance with this policy. */
  def retry[T](operation: String)(f: => T): T

  /** Executes an asynchronous operation, handling errors in accordance with this policy. */
  def retryAsync[T, U](operation: String, callback: Callback[T])(f: Callback[T] => U): Unit

}

/**
 * Common retry policy implementations.
 */
object RetryPolicy {

  /** An exception that will not cause policy implementations to issue any warning messages. */
  class RetrySilentlyException extends RuntimeException

  /** A policy that will not attempt to retry failed operations. */
  object NoRetry extends RetryPolicy {

    /** @inheritdoc */
    override def retry[T](operation: String)(f: => T) = f

    /** @inheritdoc */
    override def retryAsync[T, U](operation: String, callback: Callback[T])(f: Callback[T] => U) {
      f(callback)
    }

  }

  /**
   * A policy that will attempt an operation repeatedly with an increasing sleep period between attempts and
   * ultimately terminate after a specified duration.
   */
  class FibonacciBackoff(val retryDuration: (Long, TimeUnit), val log: Logger) extends RetryPolicy {

    /** @inheritdoc */
    override def retry[T](operation: String)(f: => T): T = {
      val endAt = System.currentTimeMillis + retryDuration._2.toMillis(retryDuration._1)
      var backoff = 100L
      var lastException: Exception = null
      do {
        try return f catch {
          case e: AmazonServiceException if e.getErrorType == AmazonServiceException.ErrorType.Client =>
            logInterrupted(operation, e)
            throw e
          case e: Exception =>
            lastException = e
            val sleepFor = math.min(backoff, math.max(1, endAt - System.currentTimeMillis))
            logRetrying(operation, e, sleepFor)
            Thread.sleep(sleepFor)
            backoff = nextBackoff(backoff)
        }
      } while (endAt > System.currentTimeMillis)
      logAborting(operation, lastException)
      throw lastException
    }

    /** @inheritdoc */
    override def retryAsync[T, U](operation: String, callback: Callback[T])(f: Callback[T] => U) {
      new Operation(operation, f, callback)()
    }

    /** Calculates the next back-off duration from the current back-off duration. */
    private def nextBackoff(backoff: Long) =
      math.min(backoff * 8 / 5, 10 * 60 * 1000L)

    /** Logs a warning about the specified exception if it is not marked as silent. */
    private def logRetrying(operation: String, thrown: Exception, sleepFor: Long) {
      if (!thrown.isInstanceOf[RetrySilentlyException]) {
        log.warn("Exception during %s:\n%s: %s" format
          (operation, thrown.getClass.getName, Option(thrown.getMessage) getOrElse ""))
        log.warn("Sleeping " + sleepFor + "ms ...")
      }
    }

    /** Logs an error about the specified interrupting exception. */
    private def logInterrupted(operation: String, thrown: Exception) {
      log.error("Operation %s interrupted." format operation, thrown.getCause)
    }

    /** Logs an error about the specified aborting exception. */
    private def logAborting(operation: String, thrown: Exception) {
      log.error("Too many exception during %s; aborting" format operation, thrown)
    }

    /** Implementation of the asynchronous retry behavior. */
    private final class Operation[T](name: String, f: Callback[T] => _, callback: Callback[T]) extends Callback[T] {

      /** The time to abort retry attempts. */
      private var endAt = 0L
      /** The amount of time to pause between attempts. */
      private var backoff = 100L

      /** Attempts to perform the operation. */
      def apply() {
        endAt = System.currentTimeMillis + retryDuration._2.toMillis(retryDuration._1)
        f(this)
      }

      /** @inheritdoc */
      override def onSuccess(result: T) {
        callback.onSuccess(result)
      }

      /** @inheritdoc */
      override def onError(thrown: Exception) {
        val remaining = endAt - System.currentTimeMillis
        thrown match {
          case e: AmazonServiceException if e.getErrorType == AmazonServiceException.ErrorType.Client =>
            logInterrupted(name, e)
            callback.onError(e)
          case e if remaining <= 0 =>
            logAborting(name, e)
            callback.onError(e)
          case e =>
            val sleepFor = math.min(backoff, math.max(1, remaining))
            logRetrying(name, e, sleepFor)
            Thread.sleep(sleepFor)
            backoff = nextBackoff(backoff)
            f(this)
        }
      }

    }

  }

}