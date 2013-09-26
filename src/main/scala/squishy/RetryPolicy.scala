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
import util.control.NonFatal
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

  /**
   * Extracts only errors that interrupt retry attempts.
   */
  object FatalError {
    def unapply(thrown: Throwable): Option[Throwable] = thrown match {
      case e: AmazonServiceException if e.getErrorType == AmazonServiceException.ErrorType.Client => Some(e)
      case NonFatal(e) => None
      case e => Some(e)
    }
  }

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
  class FibonacciBackoff(
    val retryDuration: Duration = Duration.Inf,
    val logger: Logger = Logger.Console)
    extends RetryPolicy {

    /** @inheritdoc */
    override def retry[T](operation: String)(f: => T): T = {
      val startAt = now
      var backoff = 100L
      while (true) {
        try return f catch {
          case FatalError(e) =>
            logInterrupted(operation, e)
            throw e
          case e if (now + backoff - startAt).millis < retryDuration =>
            logRetrying(operation, e, backoff)
            sleep(backoff)
            backoff = nextBackoff(backoff)
          case e: Throwable =>
            logAborting(operation, e)
            throw e
        }
      }
      sys.error("unreachable")
    }

    /** @inheritdoc */
    override def retryAsync[T](operation: String)(f: => Future[T])(implicit context: ExecutionContext) = {
      val outcome = Promise[T]()
      val startAt = now
      var backoff = 100L
      lazy val callback: Try[T] => Unit = {
        case Success(result) =>
          outcome.success(result)
        case Failure(thrown) =>
          thrown match {
            case FatalError(e) =>
              logInterrupted(operation, e)
              outcome.failure(e)
            case e if (now + backoff - startAt).millis < retryDuration =>
              logRetrying(operation, e, backoff)
              sleep(backoff)
              backoff = nextBackoff(backoff)
              f.onComplete(callback)
            case e =>
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
    private def nextBackoff(backoff: Long) = backoff * 8 / 5

    /** Logs a warning about the specified exception if it is not marked as silent. */
    private def logRetrying(operation: String, thrown: Throwable, sleepFor: Long) =
      if (!thrown.isInstanceOf[RetrySilentlyException]) {
        val name = thrown.getClass.getName
        val msg = Option(thrown.getMessage) getOrElse ""
        logger.warn(s"""|Exception during $operation: $name: $msg
                     |Sleeping $sleepFor ms ...""".stripMargin)
      }

    /** Logs an error about the specified interrupting exception. */
    private def logInterrupted(operation: String, thrown: Throwable) =
      logger.error(s"Operation $operation interrupted.", thrown)

    /** Logs an error about the specified aborting exception. */
    private def logAborting(operation: String, thrown: Throwable) =
      logger.error(s"Too many exception during $operation... aborting.", thrown)

  }

}