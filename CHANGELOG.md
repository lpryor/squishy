================
1.0 (2013-05-17)
================

Initial release.


================
1.1 (2013-06-13)
================

Added support for the batch send, change visibility & delete operations.


==================
1.1.1 (2013-06-27)
==================

Support for the new maximum message size (256 KB).


================
1.2 (2013-08-05)
================

Moving to Scala 2.10.


================
2.0 (2013-09-XX)
================

API updates to integrate cleanly with Scala 2.10:
  - Moved from using `scala.parallel.Future` to `scala.concurrent.Future`
  - Completely removed `squishy.Callback` in favor of using the operations on `scala.concurrent.Future`
  - Changed the `squishy.AsyncQueue` API match that of `squishy.SyncQueue`
  - Moved from using `(Long, java.util.concurrent.TimeUnit)` to `scala.concurrent.Duration`
  - When an operation is performed on a non-existent queue, Squishy now produces a
    `com.amazonaws.services.sqs.model.QueueDoesNotExistException` instead of a `java.lang.IllegalStateException`
  
Internal changes to make use of Scala 2.10 features:
  - Internal use of the `scala.concurrent` packages for implementing asynchronous behavior
  - Switch to using string interpolation as opposed to `String.format`
  
Other Improvements:
  - Added tests for `squishy.RetryPolicy`
  - General code cleanup and removal of boilerplate
  - Reworked the examples to be safer and more idiomatic.
