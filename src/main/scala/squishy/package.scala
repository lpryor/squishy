/* package.scala
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

/**
 * Squishy provides an idiomatic Scala API for the [[http://aws.amazon.com/sqs/ Amazon Simple Queue Service]] (SQS).
 *
 * Squishy provides two views on a SQS queue, [[squishy.SyncQueue]] and [[squishy.AsyncQueue]], that expose the
 * synchronous and asynchronous SQS APIs respectively. Views are created by extending one or both of the aforementioned
 * traits:
 * {{{
 * object MySyncQueue extends SyncQueue[String] {
 *   ...
 * }
 *
 * object MyAsyncQueue extends AsyncQueue[String] {
 *   ...
 * }
 *
 * object MySuperQueue extends SyncQueue[String] with AsyncQueue[String] {
 *   ...
 * }
 * }}}
 *
 * See [[squishy.Queue]] for information on the configuration parameters accepted by both queue types. See
 * [[squishy.SyncQueue]] or [[squishy.AsyncQueue]] for information on how to use those interfaces.
 */
package object squishy