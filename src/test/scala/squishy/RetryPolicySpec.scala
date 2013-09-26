/* RetryPolicySpec.scala
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
 */
package squishy

import concurrent.{ Await, future }
import concurrent.duration._
import concurrent.ExecutionContext.Implicits._
import com.amazonaws.AmazonServiceException
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Test suite for [[squishy.RetryPolicy]].
 */
@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class RetryPolicySpec extends FunSpec with ShouldMatchers {

  import RetryPolicy._

  val success = "Success!"

  describe("RetryPolicy.Never") {

    it("should only execute operations a single time") {
      locally {
        val tracker = new Tracker(1, 1)
        evaluating(Never.retry("fail")(tracker())) should produce[IllegalStateException]
        Never.retry("succeed")(tracker()) should be(success)
        tracker.verify()
      }
      locally {
        val tracker = new Tracker(1, 1)
        evaluating(Await.result(Never.retryAsync("fail")(tracker.async()), Duration.Inf)) should
          produce[IllegalStateException]
        Await.result(Never.retryAsync("succeed")(tracker.async()), Duration.Inf) should be(success)
        tracker.verify()
      }
    }

  }

  describe("RetryPolicy.FibonacciBackoff") {

    it("should execute operations with an increasing backoff") {
      locally {
        val tracker = new Tracker(2, 1)
        val policy = new FibonacciBackoff(5.minutes, tracker)
        policy.retry("succeed")(tracker()) should be(success)
        tracker.verify(2, 0)
      }
      locally {
        val tracker = new Tracker(2, 1)
        val policy = new FibonacciBackoff(5.minutes, tracker)
        Await.result(policy.retryAsync("succeed")(tracker.async()), Duration.Inf) should be(success)
        tracker.verify(2, 0)
      }
    }

    it("should abort execution when encountering fatal errors") {
      val error = new AmazonServiceException("error")
      error.setErrorType(AmazonServiceException.ErrorType.Client)
      locally {
        val tracker = new Tracker(1, 0, error)
        val policy = new FibonacciBackoff(5.minutes, tracker)
        evaluating(policy.retry("fail")(tracker())) should produce[AmazonServiceException]
        tracker.verify(0, 1)
      }
      locally {
        val tracker = new Tracker(1, 0, error)
        val policy = new FibonacciBackoff(5.minutes, tracker)
        evaluating(Await.result(policy.retryAsync("fail")(tracker.async()), Duration.Inf)) should
          produce[AmazonServiceException]
        tracker.verify(0, 1)
      }
    }

    it("should abort execution when reaching a configured timeout") {
      locally {
        val tracker = new Tracker(1, 0)
        val policy = new FibonacciBackoff(5.millis, tracker)
        evaluating(policy.retry("fail")(tracker())) should produce[IllegalStateException]
        tracker.verify(0, 1)
      }
      locally {
        val tracker = new Tracker(1, 0)
        val policy = new FibonacciBackoff(5.millis, tracker)
        evaluating(Await.result(policy.retryAsync("fail")(tracker.async()), Duration.Inf)) should
          produce[IllegalStateException]
        tracker.verify(0, 1)
      }
    }

  }

  /** A utility for succeeding or failing a specific number of times and counting log statements. */
  class Tracker(
    private var failures: Int = 0,
    private var successes: Int = 0,
    error: Throwable = new IllegalStateException)
    extends Logger {

    private var warnings = 0
    private var errors = 0

    def apply() =
      if (failures > 0) {
        failures -= 1
        throw error
      } else if (successes > 0) {
        successes -= 1
        success
      } else
        sys.error("Tracker called unexpectedly")

    def async() = future(apply())

    def verify(warnings: Int = 0, errors: Int = 0) = {
      failures should be(0)
      successes should be(0)
      this.warnings should be(warnings)
      this.errors should be(errors)
    }

    override def warn(message: String) = {
      warnings += 1
    }

    override def error(message: String, t: Throwable) = {
      errors += 1
    }

  }

}