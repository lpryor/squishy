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

import concurrent.{ blocking, ExecutionContext, Future, Promise }
import concurrent.duration._
import util.{ Try, Success, Failure }

import com.amazonaws.AmazonServiceException

/**
 * A plugagable retry policy for dealing with Amazon SQS errors.
 */
trait RetryPolicy {

  /** Executes a synchronous operation, handling errors in accordance with this policy. */
  def retry[T](operation: String)(f: => T): T

  /** Executes an asynchronous operation, handling errors in accordance with this policy. */
  def retryAsync[T](operation: String)(f: => Future[T])(implicit context: ExecutionContext): Future[T]

}

/**
 * Common retry policy implementations.
 */
object RetryPolicy {

  /** An exception that will not cause policy implementations to issue any warning messages. */
  class RetrySilentlyException extends RuntimeException

  /** A policy that will not attempt to retry failed operations. */
  object Never extends RetryPolicy {

    /** @inheritdoc */
    override def retry[T](operation: String)(f: => T) = f

    /** @inheritdoc */
    override def retryAsync[T](operation: String)(f: => Future[T])(implicit context: ExecutionContext) = f

  }

  /**
   * A policy that will attempt an operation repeatedly with an increasing sleep period between attempts and
   * ultimately terminate after a specified duration.
   */
  class FibonacciBackoff(val retryDuration: Duration, val log: Logger) extends RetryPolicy {

    /** @inheritdoc */
    override def retry[T](operation: String)(f: => T): T = {
      val endAt = now + retryDuration.toMillis
      var backoff = 100L
      while (true) {
        try return f catch {
          case e: AmazonServiceException if e.getErrorType == AmazonServiceException.ErrorType.Client =>
            logInterrupted(operation, e)
            throw e
          case e: Exception if now + backoff < endAt =>
            logRetrying(operation, e, backoff)
            sleep(backoff)
            backoff = nextBackoff(backoff)
          case e: Exception =>
            logAborting(operation, e)
            throw e
        }
      }
      sys.error("unreachable")
    }

    /** @inheritdoc */
    override def retryAsync[T](operation: String)(f: => Future[T])(implicit context: ExecutionContext) = {
      val outcome = Promise[T]()
      val endAt = now + retryDuration.toMillis
      var backoff = 100L
      lazy val callback: Try[T] => Unit = {
        case Success(result) =>
          outcome.success(result)
        case Failure(thrown) =>
          thrown match {
            case e: AmazonServiceException if e.getErrorType == AmazonServiceException.ErrorType.Client =>
              logInterrupted(operation, e)
              outcome.failure(e)
            case e: Exception if now + backoff < endAt =>
              logRetrying(operation, e, backoff)
              sleep(backoff)
              backoff = nextBackoff(backoff)
              f.onComplete(callback)
            case e: Exception =>
              logAborting(operation, e)
              outcome.failure(e)
          }
      }
      f.onComplete(callback)
      outcome.future
    }

    /** Returns the current time in milliseconds. */
    private def now = System.currentTimeMillis
    
    /** Sleeps for the specified number of milliseconds. */
    private def sleep(duration: Long) = blocking(Thread.sleep(duration))

    /** Calculates the next back-off duration from the current back-off duration. */
    private def nextBackoff(backoff: Long) =
      math.min(backoff * 8 / 5, 10.minutes.toMillis)

    /** Logs a warning about the specified exception if it is not marked as silent. */
    private def logRetrying(operation: String, thrown: Throwable, sleepFor: Long) =
      if (!thrown.isInstanceOf[RetrySilentlyException]) {
        val name = thrown.getClass.getName
        val msg = Option(thrown.getMessage) getOrElse ""
        log.warn(s"Exception during $operation:\n$name: $msg")
        log.warn(s"Sleeping $sleepFor ms ...")
      }

    /** Logs an error about the specified interrupting exception. */
    private def logInterrupted(operation: String, thrown: Throwable) =
      log.error(s"Operation $operation interrupted.", thrown)

    /** Logs an error about the specified aborting exception. */
    private def logAborting(operation: String, thrown: Throwable) =
      log.error(s"Too many exception during $operation... aborting.", thrown)

  }

}