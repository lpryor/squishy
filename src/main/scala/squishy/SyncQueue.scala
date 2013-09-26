/* SyncQueue.scala
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

import com.amazonaws.services.sqs.model._

/**
 * A wrapper around a synchronous SNS queue that supports idiomatic Scala.
 *
 * Note: implementations will transparently acquire and cache this queue's URL when required. See the note about queue
 * URLs in the class description of [[squishy.Queue]].
 *
 * @tparam M The type of message that can be sent to and received from this queue.
 */
trait SyncQueue[M] extends Queue[M] {

  /** Returns true if this queue exists in the cloud. */
  def exists: Boolean = queueUrl.isDefined

  /** Returns the URL of this queue if it exists or `None` if it does not. */
  def queueUrl: Option[String] =
    cachedQueueUrl match {
      case Some(queueUrl) =>
        queueUrl
      case None =>
        val request = newGetQueueUrlRequest()
        val result = retryPolicy.retry("Queue(%s).queueUrl" format queueName) {
          try {
            Some(sqsClient.getQueueUrl(request))
          } catch {
            case e: QueueDoesNotExistException => None
          }
        }
        val queueUrl = Some(result map getQueueUrlResultToQueueUrl)
        synchronized(if (cachedQueueUrl.isEmpty) cachedQueueUrl = queueUrl)
        cachedQueueUrl.get
    }

  /** Returns all of the attributes of this queue if it exists or an empty set if it does not. */
  def attributes: Queue.AttributeSet =
    attributes(Queue.keys: _*)

  /**
   * Returns the specified attributes of this queue if it exists or an empty set if it does not.
   *
   * @param keys The keys that identify the attributes to return.
   */
  def attributes(keys: Queue.Key[_]*): Queue.AttributeSet =
    queueUrl match {
      case Some(queueUrl) =>
        val request = newGetQueueAttributesRequest(queueUrl, keys: _*)
        val result = retryPolicy.retry(s"Queue($queueName).attributes")(sqsClient.getQueueAttributes(request))
        getQueueAttributesResultToAttributeSet(result)
      case None =>
        Queue.AttributeSet()
    }

  /**
   * Sets the attributes of this queue, throwing an exception if it does not exist.
   *
   * @param attributes The attributes to configure for this queue.
   */
  def attributes_=(attributes: Seq[Queue.MutableAttribute[_]]): Unit = {
    val request = newSetQueueAttributesRequest(requireQueueUrl, attributes)
    retryPolicy.retry(s"Queue($queueName).attributes = ...")(sqsClient.setQueueAttributes(request))
  }

  /**
   * Creates this queue in the cloud if it does not already exist.
   *
   * All unspecified attributes will default to the values specified by Amazon SQS.
   *
   * @param attributes The attributes to configure for this queue.
   */
  def createQueue(attributes: Queue.MutableAttribute[_]*): Unit = {
    val request = newCreateQueueRequest(attributes)
    val result = retryPolicy.retry(s"Queue($queueName).createQueue")(sqsClient.createQueue(request))
    val queueUrl = Some(Some(createQueueResultToQueueUrl(result)))
    synchronized(cachedQueueUrl = queueUrl)
  }

  /** Deletes this queue in the cloud if it exists. */
  def deleteQueue_!(): Unit = {
    val request = newDeleteQueueRequest(requireQueueUrl)
    retryPolicy.retry(s"Queue($queueName).deleteQueue_!")(sqsClient.deleteQueue(request))
    val queueUrl = Some(None)
    synchronized(cachedQueueUrl = queueUrl)
  }

  /**
   * Sends a message to this queue.
   *
   * All optional parameters of this method will default to the values specified by Amazon SQS.
   *
   * @param msg The body of the message to send.
   * @param delaySeconds The number of seconds to delay message availability.
   */
  def send(msg: M, delaySeconds: Int = -1): Message.Sent[M] = {
    val request = newSendMessageRequest(requireQueueUrl, msg, delaySeconds)
    val result = retryPolicy.retry(s"Queue($queueName).send")(sqsClient.sendMessage(request))
    sendMessageResultToMessage(result, msg)
  }

  /**
   * Sends a batch of messages to this queue.
   *
   * All optional parameters of this method will default to the values specified by Amazon SQS.
   *
   * @param entries The entries representing the messages to send. These must be of type `M` for immediate messages or
   * `(M, Int)` for messages with an initial delay.
   */
  def sendBatch[E: BatchEntry](entries: E*): Seq[Message[M]] = {
    val entry = implicitly[BatchEntry[E]]
    val messages = entries map entry
    val request = newSendMessageBatchRequest(requireQueueUrl, messages)
    val result = retryPolicy.retry(s"Queue($queueName).sendBatch")(sqsClient.sendMessageBatch(request))
    sendMessageBatchResultToMessages(result, messages)
  }

  /**
   * Attempts to receive one or more messages from this queue.
   *
   * All optional parameters of this method will default to the values specified by Amazon SQS.
   *
   * @param maxNumberOfMessages The maximum number of messages to receive.
   * @param visibilityTimeout The number of seconds to prevent other consumers from seeing received messages.
   * @param waitTimeSeconds The maximum number of seconds to wait for a message.
   * @param attributes The keys of the message attributes that should be returned along with the messages.
   */
  def receive(
    maxNumberOfMessages: Int = -1,
    visibilityTimeout: Int = -1,
    waitTimeSeconds: Int = -1,
    attributes: Seq[Message.Key[_]] = Seq.empty //
    ): Seq[Message.Receipt[M]] = {
    val request = newReceiveMessageRequest(
      requireQueueUrl,
      maxNumberOfMessages,
      visibilityTimeout,
      waitTimeSeconds,
      attributes)
    val result = retryPolicy.retry(s"Queue($queueName).receive")(sqsClient.receiveMessage(request))
    receiveMessageResultToMessages(result)
  }

  /**
   * Attempts to extend the time that a message is invisible to other consumers.
   *
   * @param receipt The receipt of the message to modify the visibility of.
   * @param visibilityTimeout The number of seconds to extends the message's visibility timeout.
   */
  def changeVisibility(receipt: Message.Receipt[M], visibilityTimeout: Int): Message.Changed[M] = {
    val request = newChangeMessageVisibilityRequest(requireQueueUrl, receipt, visibilityTimeout)
    retryPolicy.retry(s"Queue($queueName).changeVisibility")(sqsClient.changeMessageVisibility(request))
    changeMessageVisibilityResultToMessage(receipt)
  }

  /**
   * Attempts to extend the time that a batch of messages is invisible to other consumers.
   *
   * @param entries The entries representing the messages to change the visibility of with their new visibility timeout.
   */
  def changeVisibilityBatch(entries: (Message.Receipt[M], Int)*): Seq[Message[M]] = {
    val request = newChangeMessageVisibilityBatchRequest(requireQueueUrl, entries)
    val result = retryPolicy.retry(s"Queue($queueName).changeVisibilityBatch") {
      sqsClient.changeMessageVisibilityBatch(request)
    }
    changeMessageVisibilityBatchResultToMessages(result, entries)
  }

  /**
   * Attempts to delete a message from this queue.
   *
   * @param receipt The receipt of the message to delete from the queue.
   */
  def delete(receipt: Message.Receipt[M]): Message.Deleted[M] = {
    val request = newDeleteMessageRequest(requireQueueUrl, receipt)
    retryPolicy.retry(s"Queue($queueName).delete")(sqsClient.deleteMessage(request))
    deleteMessageResultToMessage(receipt)
  }

  /**
   * Attempts to delete a batch of messages from this queue.
   *
   * @param receipts The receipts of the messages to delete from the queue.
   */
  def deleteBatch(receipts: Message.Receipt[M]*): Seq[Message[M]] = {
    val request = newDeleteMessageBatchRequest(requireQueueUrl, receipts)
    val result = retryPolicy.retry(s"Queue($queueName).deleteBatch")(sqsClient.deleteMessageBatch(request))
    deleteMessageBatchResultToMessages(result, receipts)
  }

  /** Returns the queue URL or throws an exception if it does not exist. */
  private def requireQueueUrl: String =
    queueUrl getOrElse { throw new QueueDoesNotExistException(s"Queue $queueName does not exist.") }

}