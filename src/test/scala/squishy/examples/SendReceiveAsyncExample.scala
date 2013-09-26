/* SendReceiveAsyncExample.scala
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
package examples

import concurrent.{ Await, Future, ExecutionContext }
import concurrent.duration._
import io.Source

/**
 * A simple example that shows sending and receiving asynchronously.
 */
object SendReceiveAsyncExample extends App {

  // Logs any errors.
  val logErrors: PartialFunction[Throwable, Unit] = { case e => e.printStackTrace() }

  // Create a queue and ensure that it exists in the cloud.
  val queue = new MyQueue
  if (!queue.exists)
    queue.createQueue(Queue.ReceiveMessageWaitTimeSeconds -> 1)

  // Use the queue's thread pool.
  import queue.executionContext

  // Read from stdin and send any lines as messages until terminated.
  val stdin = Source.fromInputStream(System.in).getLines()
  def publish(): Future[Unit] = {
    (if (stdin.hasNext) stdin.next else "exit") match {
      case "exit" =>
        Future.successful(())
      case "error" =>
        Future.failed(new RuntimeException)
      case text =>
        queue.sendAsync(MyMessage(text)) flatMap (_ => publish())
    }
  }

  // Continuously receive messages and print to stdout until terminated.
  @volatile
  var terminated = false
  def consume(): Future[Unit] = {
    queue.receiveAsync() flatMap { receipts =>
      receipts foreach (r => println(r.body.text))
      if (receipts.nonEmpty) queue.deleteBatch(receipts: _*)
      if (terminated) Future.successful(())
      else consume()
    } recover logErrors
  }

  // Start the publisher an consumer processes and wait for them to complete before shutting down.
  Await.ready(publish() recover logErrors map (_ => terminated = true) zip consume(), Duration.Inf)
  queue.sqsClient.shutdown()

  /**
   * A simple custom message class.
   */
  final case class MyMessage(text: String)

  /**
   * Our configured queue implementation
   */
  class MyQueue extends SyncQueue[MyMessage] with AsyncQueue[MyMessage] {

    override val queueName = "Example"

    override val sqsClient = new fake.FakeSQSAsync

    override implicit val executionContext = ExecutionContext.fromExecutor(sqsClient.executor)

    override val messageMapper = new Mapper[MyMessage] {
      override def apply(msg: MyMessage) = msg.text
      override def unapply(text: String) = MyMessage(text)
    }

  }

}