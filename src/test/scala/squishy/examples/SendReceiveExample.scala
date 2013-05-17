/* SendReceiveExample.scala
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

/**
 * A simple example that shows sending and receiving in separate threads.
 */
object SendReceiveExample extends App {

  @volatile
  var terminated = false

  // Create a queue and ensure that it exists in the cloud.
  val queue = new MyQueue
  if (!queue.exists)
    queue.createQueue()

  // Launch a thread to read messages and print them to stdout.
  new Thread(new Runnable() {
    override def run() {
      while (!terminated)
        queue.receive() foreach { receipt =>
          println(receipt.body.text)
          queue.delete(receipt)
        }
    }
  }).start()

  // Read from stdin and send any lines as messages.
  val reader = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))
  while (!terminated) {
    val text = reader.readLine()
    if (text == null || text == "exit")
      terminated = true
    else
      queue.send(MyMessage(text))
  }

  /**
   * A simple custom message class.
   */
  final case class MyMessage(text: String)

  /**
   * Our configured queue implementation
   */
  class MyQueue extends SyncQueue[MyMessage] {

    override val queueName = "Example"

    override val retryPolicy = RetryPolicy.NoRetry

    override val sqsClient = new fake.FakeSQS

    override val messageMapper = new Mapper[MyMessage] {
      override def apply(msg: MyMessage) = msg.text
      override def unapply(text: String) = MyMessage(text)
    }

  }

}