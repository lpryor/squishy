/* FakeSQS.scala
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
package squishy.fake

import com.amazonaws.{
  AmazonClientException,
  AmazonWebServiceRequest
}
import com.amazonaws.regions.Region
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model._

import collection.JavaConverters._
import collection.mutable.HashMap

import language.implicitConversions

/**
 * A simple implementation of the Amazon SQS interface for testing purposes.
 */
class FakeSQS extends AmazonSQS {

  import FakeSQS._

  /** The index of fake queues by URL. */
  private val queues = HashMap[String, FakeQueue]()
  /** Dummy client endpoint. */
  private var _endpoint: String = _
  /** Dummy client region. */
  private var _region: Region = _
  /** True if this client has been shut down. */
  private var isShutdown = false

  /** Returns the configured endpoint. */
  def endpoint = _endpoint

  /** Returns the configured region. */
  def region = _region

  override def setEndpoint(endpoint: String) {
    this._endpoint = endpoint
  }

  override def setRegion(region: Region) {
    this._region = region
  }

  override def createQueue(request: CreateQueueRequest) = synchronized {
    requireNotShutdown()
    val queueName = request.getQueueName
    val attributes = defaultAttributes ++ request.getAttributes.asScala
    if (attributes.size != defaultAttributes.size)
      throw new InvalidAttributeNameException(attributes.keySet -- defaultAttributes.keySet mkString ", ")
    queues.values find (_.name == queueName) match {
      case Some(queue) =>
        if (queue.checkAttributes(attributes))
          new CreateQueueResult().withQueueUrl(queue.url)
        else
          throw new QueueNameExistsException(queueName)
      case None =>
        val queue = new FakeQueue(queueName)
        queue.configureAttributes(attributes)
        queues += queue.url -> queue
        new CreateQueueResult().withQueueUrl(queue.url)
    }
  }

  override def listQueues() = synchronized {
    requireNotShutdown()
    new ListQueuesResult().withQueueUrls(queues.values.map(_.url).asJavaCollection)
  }

  override def listQueues(request: ListQueuesRequest) = synchronized {
    requireNotShutdown()
    val prefix = request.getQueueNamePrefix
    new ListQueuesResult().withQueueUrls(queues.filter(_._1 startsWith prefix).values.map(_.url).asJavaCollection)
  }

  override def deleteQueue(request: DeleteQueueRequest) = synchronized {
    requireNotShutdown()
    if (queues.remove(request.getQueueUrl).isEmpty)
      throw new QueueDoesNotExistException(request.getQueueUrl)
  }

  override def getQueueUrl(request: GetQueueUrlRequest) = synchronized {
    requireNotShutdown()
    queues.values find (_.name == request.getQueueName) match {
      case Some(queue) =>
        new GetQueueUrlResult().withQueueUrl(queue.url)
      case None =>
        throw new QueueDoesNotExistException(request.getQueueName)
    }
  }

  override def getQueueAttributes(request: GetQueueAttributesRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val attributeNames = request.getAttributeNames.asScala.toSet
    if (!(attributeNames forall defaultAttributes.keySet))
      throw new InvalidAttributeNameException(attributeNames -- defaultAttributes.keySet mkString ", ")
    var attributes = queue.attributes
    if (!(attributeNames contains all))
      attributes = attributes filter (attributeNames contains _._1)
    new GetQueueAttributesResult().withAttributes(attributes.asJava)
  }

  override def setQueueAttributes(request: SetQueueAttributesRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val attributes = request.getAttributes.asScala
    if (!(attributes.keySet forall defaultAttributes.keySet))
      throw new InvalidAttributeNameException(attributes.keySet -- defaultAttributes.keySet mkString ", ")
    queue.configureAttributes(attributes)
  }

  override def sendMessage(request: SendMessageRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val messageSize = request.getMessageBody.getBytes("UTF-8").length
    if (messageSize > 262144)
      throw new InvalidMessageContentsException("Message is too large: " + messageSize)
    queue.send(request.getMessageBody, Option(request.getDelaySeconds) map (_.longValue)) match {
      case Some(msg) =>
        new SendMessageResult()
          .withMessageId(msg.id.toString)
          .withMD5OfMessageBody(msg.md5)
      case None =>
        throw new InvalidMessageContentsException("Invalid message: " + request.getMessageBody)
    }
  }

  override def sendMessageBatch(request: SendMessageBatchRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val entries = request.getEntries.asScala
    checkBatch(entries)(_.getId)
    val messagesSize = entries.map(_.getMessageBody.getBytes("UTF-8").length).sum
    if (messagesSize > 262144)
      throw new BatchRequestTooLongException("Messages combined are too long: " + messagesSize)
    var successful = Vector.empty[SendMessageBatchResultEntry]
    var failed = Vector.empty[BatchResultErrorEntry]
    for (entry <- entries) {
      queue.send(entry.getMessageBody, Option(entry.getDelaySeconds) map (_.longValue)) match {
        case Some(msg) =>
          successful :+= new SendMessageBatchResultEntry()
            .withId(entry.getId)
            .withMessageId(msg.id.toString)
            .withMD5OfMessageBody(msg.md5)
        case None =>
          failed :+= new BatchResultErrorEntry()
            .withId(entry.getId)
            .withCode("InvalidMessageContents")
            .withMessage("Invalid message: " + entry.getMessageBody)
            .withSenderFault(true)
      }
    }
    new SendMessageBatchResult()
      .withSuccessful(successful.asJava)
      .withFailed(failed.asJava)
  }

  override def receiveMessage(request: ReceiveMessageRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val attributeNames = request.getAttributeNames.asScala.toSet
    val messages = queue.receive(
      Option(request.getMaxNumberOfMessages) map (_.intValue) getOrElse 1,
      Option(request.getWaitTimeSeconds) map (_.longValue),
      Option(request.getVisibilityTimeout) map (_.longValue)
    ) collect {
        case (msg, receipt) => new Message()
          .withMessageId(msg.id.toString)
          .withMD5OfBody(msg.md5)
          .withBody(msg.content)
          .withReceiptHandle(receipt)
          .withAttributes(msg.attributes.filter(attributeNames contains _._1).asJava)
      }
    new ReceiveMessageResult().withMessages(messages.asJava)
  }

  override def changeMessageVisibility(request: ChangeMessageVisibilityRequest) = {
    val queue = getQueue(request.getQueueUrl)
    if (queue.changeVisibility(
      request.getReceiptHandle,
      Option(request.getVisibilityTimeout) map (_.longValue) getOrElse 0L).isEmpty)
      throw new ReceiptHandleIsInvalidException(
        "Unable to change the visibility of message with receipt: " + request.getReceiptHandle)
  }

  override def changeMessageVisibilityBatch(request: ChangeMessageVisibilityBatchRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val entries = request.getEntries.asScala
    checkBatch(entries)(_.getId)
    var successful = Vector.empty[ChangeMessageVisibilityBatchResultEntry]
    var failed = Vector.empty[BatchResultErrorEntry]
    for (entry <- entries) {
      if (queue.changeVisibility(
        entry.getReceiptHandle,
        Option(entry.getVisibilityTimeout) map (_.longValue) getOrElse 0L).isDefined)
        successful :+= new ChangeMessageVisibilityBatchResultEntry().withId(entry.getId)
      else
        failed :+= new BatchResultErrorEntry()
          .withId(entry.getId)
          .withCode("ReceiptHandleIsInvalidException")
          .withMessage("Unable to change the visibility of message with receipt: " + entry.getReceiptHandle)
          .withSenderFault(true)
    }
    new ChangeMessageVisibilityBatchResult()
      .withSuccessful(successful.asJava)
      .withFailed(failed.asJava)
  }

  override def deleteMessage(request: DeleteMessageRequest) = {
    val queue = getQueue(request.getQueueUrl)
    if (queue.delete(request.getReceiptHandle).isEmpty)
      throw new ReceiptHandleIsInvalidException("Unable to delete the message with receipt: " + request.getReceiptHandle)
  }

  override def deleteMessageBatch(request: DeleteMessageBatchRequest) = {
    val queue = getQueue(request.getQueueUrl)
    val entries = request.getEntries.asScala
    checkBatch(entries)(_.getId)
    var successful = Vector.empty[DeleteMessageBatchResultEntry]
    var failed = Vector.empty[BatchResultErrorEntry]
    for (entry <- entries) {
      if (queue.delete(entry.getReceiptHandle).isDefined)
        successful :+= new DeleteMessageBatchResultEntry().withId(entry.getId)
      else
        failed :+= new BatchResultErrorEntry()
          .withId(entry.getId)
          .withCode("ReceiptHandleIsInvalidException")
          .withMessage("Unable to delete the message with receipt: " + entry.getReceiptHandle)
          .withSenderFault(true)
    }
    new DeleteMessageBatchResult()
      .withSuccessful(successful.asJava)
      .withFailed(failed.asJava)
  }

  override def addPermission(request: AddPermissionRequest) =
    throw new UnsupportedOperationException

  override def removePermission(request: RemovePermissionRequest) =
    throw new UnsupportedOperationException

  override def shutdown() {
    synchronized {
      if (isShutdown)
        return
      isShutdown = true
    }
    dispose()
    synchronized {
      queues.clear()
    }
  }

  override def getCachedResponseMetadata(request: AmazonWebServiceRequest) =
    throw new UnsupportedOperationException

  /** Internal dispose method called when this interface is shut down. */
  protected def dispose() {}

  /** Throws an exception if this client has been shut down. */
  private def requireNotShutdown() {
    if (isShutdown)
      throw new AmazonClientException("This SQS interface has been shut down.")
  }

  /** Returns the queue with the specified URL if this client has not been shut down. */
  private def getQueue(queueUrl: String) = synchronized {
    requireNotShutdown()
    queues get queueUrl getOrElse {
      throw new QueueDoesNotExistException(queueUrl)
    }
  }

  /** Checks invariants that apply to all batch operations. */
  private def checkBatch[T](entries: Seq[T])(getId: T => String): Unit = {
    if (entries.isEmpty)
      throw new EmptyBatchRequestException("Empty batch")
    if (entries.size > maxBatchSize)
      throw new TooManyEntriesInBatchRequestException("Too many entries in batch: " + entries.size)
    val entryIds = entries.map(getId).toSet
    if (entryIds.size != entries.size)
      throw new BatchEntryIdsNotDistinctException("Duplicate entry IDs: "
        + (entries.map(getId).toBuffer -- entryIds).toSet.mkString(", "))
  }

}

/**
 * Utilities for the fake Amazon SQS implementation.
 */
object FakeSQS {

  /** The "All" attribute name. */
  val all = "All"

  /** The maximum size of any batch. */
  val maxBatchSize = 10

  /** The names and default values of the mutable queue attributes. */
  val defaultAttributes = Map(
    QueueAttributeName.DelaySeconds.name -> "0",
    QueueAttributeName.MaximumMessageSize.name -> "262144",
    QueueAttributeName.MessageRetentionPeriod.name -> "345600",
    QueueAttributeName.Policy.name -> "",
    QueueAttributeName.ReceiveMessageWaitTimeSeconds.name -> "0",
    QueueAttributeName.VisibilityTimeout.name -> "30"
  )

  /** Adds the AWS-specific operations to queues. */
  implicit def queueToQueueOps(queue: FakeQueue): QueueOps = new QueueOps(queue)

  /** Adds the AWS-specific operations to messages. */
  implicit def messageToMessageOps(message: FakeQueue.Message): MessageOps = new MessageOps(message)

  /**
   * AWS-specific operations added to fake queues.
   */
  class QueueOps(queue: FakeQueue) {

    /** Returns true if all the specified attributes match the values on the supplied queue. */
    def attributes = Map[String, String](
      QueueAttributeName.ApproximateNumberOfMessages.name ->
        queue.approximateNumberOfMessages.toString,
      QueueAttributeName.ApproximateNumberOfMessagesDelayed.name ->
        queue.approximateNumberOfMessagesDelayed.toString,
      QueueAttributeName.ApproximateNumberOfMessagesNotVisible.name ->
        queue.approximateNumberOfMessagesNotVisible.toString,
      QueueAttributeName.CreatedTimestamp.name ->
        queue.createdTimestamp.toString,
      QueueAttributeName.DelaySeconds.name ->
        queue.delaySeconds.toString,
      QueueAttributeName.LastModifiedTimestamp.name ->
        queue.lastModifiedTimestamp.toString,
      QueueAttributeName.MaximumMessageSize.name ->
        queue.maximumMessageSize.toString,
      QueueAttributeName.MessageRetentionPeriod.name ->
        queue.messageRetentionPeriod.toString,
      QueueAttributeName.Policy.name ->
        queue.policy,
      QueueAttributeName.QueueArn.name ->
        queue.arn,
      QueueAttributeName.ReceiveMessageWaitTimeSeconds.name ->
        queue.receiveMessageWaitTimeSeconds.toString,
      QueueAttributeName.VisibilityTimeout.name ->
        queue.visibilityTimeout.toString
    )

    /** Returns true if all the specified attributes match the values on the supplied queue. */
    def checkAttributes(attributes: Map[String, String]) =
      attributes.keySet forall {
        case key if key == QueueAttributeName.DelaySeconds.name =>
          attributes(key).toLong == queue.delaySeconds
        case key if key == QueueAttributeName.MaximumMessageSize.name =>
          attributes(key).toLong == queue.maximumMessageSize
        case key if key == QueueAttributeName.MessageRetentionPeriod.name =>
          attributes(key).toLong == queue.messageRetentionPeriod
        case key if key == QueueAttributeName.Policy.name =>
          attributes(key) == queue.policy
        case key if key == QueueAttributeName.ReceiveMessageWaitTimeSeconds.name =>
          attributes(key).toLong == queue.receiveMessageWaitTimeSeconds
        case key if key == QueueAttributeName.VisibilityTimeout.name =>
          attributes(key).toLong == queue.visibilityTimeout
      }

    /** Sets all the specified attributes on the supplied queue. */
    def configureAttributes(attributes: collection.Map[String, String]) {
      attributes.keySet foreach {
        case key if key == QueueAttributeName.DelaySeconds.name =>
          queue.delaySeconds = attributes(key).toLong
        case key if key == QueueAttributeName.MaximumMessageSize.name =>
          queue.maximumMessageSize = attributes(key).toLong
        case key if key == QueueAttributeName.MessageRetentionPeriod.name =>
          queue.messageRetentionPeriod = attributes(key).toLong
        case key if key == QueueAttributeName.Policy.name =>
          queue.policy = attributes(key)
        case key if key == QueueAttributeName.ReceiveMessageWaitTimeSeconds.name =>
          queue.receiveMessageWaitTimeSeconds = attributes(key).toLong
        case key if key == QueueAttributeName.VisibilityTimeout.name =>
          queue.visibilityTimeout = attributes(key).toLong
      }

    }

  }

  /**
   * AWS-specific operations added to fake messages.
   */
  class MessageOps(message: FakeQueue.Message) {

    /** Returns true if all the specified attributes match the values on the supplied queue. */
    def attributes = Map[String, String](
      "SenderId" -> message.senderId,
      "ApproximateFirstReceiveTimestamp" -> message.approximateFirstReceiveTimestamp.get.toString,
      "ApproximateReceiveCount" -> message.approximateReceiveCount.toString,
      "SentTimestamp" -> message.sentTimestamp.toString
    )

  }

}