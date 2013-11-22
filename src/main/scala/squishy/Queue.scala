/* Queue.scala
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

import scala.collection.JavaConverters._
import atmos.retries.{ TerminationPolicy, RetryPolicy }
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model._

/**
 * Base type for views onto SQS queues.
 *
 * This trait provides the common infrastructure required to interact with SQS and exposes the relevant configuration
 * parameters. When creating instances of this trait values for the `sqsClient`, `queueName` and `messageMapper`
 * members must be provided while `queueOwner` and `retryPolicy` are optional.
 *
 * A note about queue URLs: most SQS operations require that the caller specify a queue URL with each request. This
 * queue URL is generated by Amazon and associated with the queue name in the cloud. Thus, one of the first operations
 * when using SQS must be a call to `getQueueUrl` to obtain this value for use in subsequent operations.
 *
 * Implementations of this class will transparently acquire and cache the queue URL when it is required for other
 * operations. As a side-effect of this strategy, if the URL associated with a queue name changes (for example, by
 * deleting and re-creating a queue through the web UI), all instances of this trait that may have a previously cached
 * queue URL must be re-created as well. The queue creation and deletion operations provided by implementations of this
 * trait will correctly update the cached queue URL value.
 *
 * @tparam M The type of message that can be sent to and received from this queue.
 */
trait Queue[M] {

  /** User-provided, pre-configured SQS client implementation. */
  val sqsClient: AmazonSQS

  /** The name of the queue to be accessed in the Amazon SQS system. */
  val queueName: String

  /** The strategy for encoding and decoding message objects of type `M`. */
  val messageMapper: Mapper[M]

  /**
   * The AWS account ID of the owner of the queue to access. Defaults to `scala.None`, which will use the account
   * associated with `sqsClient`.
   */
  lazy val queueOwner: Option[String] = None

  /** A strategy for handling errors when interacting with SQS. Defaults to [[squishy.RetryPolicy.Never]]. */
  lazy val retryPolicy: RetryPolicy = Queue.defaultRetryPolicy

  /** The cached queue URL. */
  @volatile
  private[squishy] var cachedQueueUrl: Option[Option[String]] = None

  /** Utility method that constructs a `GetQueueUrlRequest` instance. */
  protected def newGetQueueUrlRequest(): GetQueueUrlRequest =
    new GetQueueUrlRequest()
      .withQueueName(queueName)
      .withQueueOwnerAWSAccountId(queueOwner.orNull)

  /** Utility method that extracts a `GetQueueUrlResult` instance. */
  protected def getQueueUrlResultToQueueUrl(result: GetQueueUrlResult): String = result.getQueueUrl

  /** Utility method that constructs a `GetQueueAttributesRequest` instance. */
  protected def newGetQueueAttributesRequest(queueUrl: String, keys: Queue.Key[_]*): GetQueueAttributesRequest =
    new GetQueueAttributesRequest()
      .withQueueUrl(queueUrl)
      .withAttributeNames(keys.map(_.name).asJavaCollection)

  /** Utility method that extracts from a `GetQueueAttributesResult` instance. */
  protected def getQueueAttributesResultToAttributeSet(result: GetQueueAttributesResult): Queue.AttributeSet =
    Queue.AttributeSet(result.getAttributes.asScala)

  /** Utility method that constructs a `SetQueueAttributesRequest` instance. */
  protected def newSetQueueAttributesRequest(
    queueUrl: String,
    attributes: Seq[Queue.MutableAttribute[_]] //
    ): SetQueueAttributesRequest =
    new SetQueueAttributesRequest()
      .withQueueUrl(queueUrl)
      .withAttributes(Queue.AttributeSet.unapply(attributes: _*).get.asJava)

  /** Utility method that constructs a `CreateQueueRequest` instance. */
  protected def newCreateQueueRequest(attributes: Seq[Queue.MutableAttribute[_]]): CreateQueueRequest =
    new CreateQueueRequest()
      .withQueueName(queueName)
      .withAttributes(Queue.AttributeSet.unapply(attributes: _*).get.asJava)

  /** Utility method that extracts a `CreateQueueResult` instance. */
  protected def createQueueResultToQueueUrl(result: CreateQueueResult): String = result.getQueueUrl

  /** Utility method that constructs a `DeleteQueueRequest` instance. */
  protected def newDeleteQueueRequest(queueUrl: String): DeleteQueueRequest =
    new DeleteQueueRequest().withQueueUrl(queueUrl)

  /** Utility method that constructs a `SendMessageRequest` instance. */
  protected def newSendMessageRequest(queueUrl: String, msg: M, delaySeconds: Int): SendMessageRequest = {
    val request = new SendMessageRequest()
      .withQueueUrl(queueUrl)
      .withMessageBody(messageMapper(msg))
    if (delaySeconds >= 0) request.setDelaySeconds(delaySeconds)
    request
  }

  /** Utility method that extracts a `SendMessageResult` instance. */
  protected def sendMessageResultToMessage(result: SendMessageResult, msg: M): Message.Sent[M] =
    Message.Sent(result.getMessageId, result.getMD5OfMessageBody, msg)

  /** Utility method that constructs a `SendMessageBatchRequest` instance. */
  protected def newSendMessageBatchRequest(queueUrl: String, entries: Seq[(M, Int)]): SendMessageBatchRequest =
    new SendMessageBatchRequest()
      .withQueueUrl(queueUrl)
      .withEntries(entries.zipWithIndex.map {
        case ((msg, delaySeconds), id) =>
          val entry = new SendMessageBatchRequestEntry()
            .withId(id.toString)
            .withMessageBody(messageMapper(msg))
          if (delaySeconds >= 0) entry.setDelaySeconds(delaySeconds)
          entry
      }.asJava)

  /** Utility method that extracts a `SendMessageBatchResult` instance. */
  protected def sendMessageBatchResultToMessages(
    result: SendMessageBatchResult,
    entries: Seq[(M, Int)] //
    ): Seq[Message[M]] = {
    val messages = entries.toIndexedSeq
    val results = collection.mutable.IndexedSeq.fill[Message[M]](messages.length)(null)
    result.getSuccessful.asScala foreach { e =>
      val id = e.getId.toInt
      results(id) = Message.Sent(e.getMessageId, e.getMD5OfMessageBody, messages(id)._1)
    }
    result.getFailed.asScala foreach { e =>
      val id = e.getId.toInt
      results(id) = Message.Error(e.getCode, e.getMessage, e.getSenderFault, messages(id)._1)
    }
    results
  }

  /** Utility method that constructs a `ReceiveMessageRequest` instance. */
  protected def newReceiveMessageRequest(
    queueUrl: String,
    maxNumberOfMessages: Int,
    visibilityTimeout: Int,
    waitTimeSeconds: Int,
    attributes: Seq[Message.Key[_]] //
    ): ReceiveMessageRequest = {
    val request = new ReceiveMessageRequest().withQueueUrl(queueUrl)
    if (maxNumberOfMessages >= 0) request.setMaxNumberOfMessages(maxNumberOfMessages)
    if (visibilityTimeout >= 0) request.setVisibilityTimeout(visibilityTimeout)
    if (waitTimeSeconds >= 0) request.setWaitTimeSeconds(waitTimeSeconds)
    if (attributes.nonEmpty) request.setAttributeNames(attributes.map(_.name).asJavaCollection)
    request
  }

  /** Utility method that extracts a `ReceiveMessageResult` instance. */
  protected def receiveMessageResultToMessages(result: ReceiveMessageResult): Seq[Message.Receipt[M]] =
    result.getMessages.asScala.map { m =>
      Message.Receipt(
        m.getMessageId,
        m.getMD5OfBody,
        m.getReceiptHandle,
        Message.AttributeSet(m.getAttributes.asScala),
        messageMapper.unapply(m.getBody))
    }

  /** Utility method that constructs a `ChangeMessageVisibilityRequest` instance. */
  protected def newChangeMessageVisibilityRequest(
    queueUrl: String,
    receipt: Message.Receipt[M],
    visibilityTimeout: Int //
    ): ChangeMessageVisibilityRequest =
    new ChangeMessageVisibilityRequest()
      .withQueueUrl(queueUrl)
      .withReceiptHandle(receipt.handle)
      .withVisibilityTimeout(visibilityTimeout)

  /** Utility method that handles the result of a `ChangeMessageVisibilityRequest` operation. */
  protected def changeMessageVisibilityResultToMessage(receipt: Message.Receipt[M]): Message.Changed[M] =
    Message.Changed(receipt.body)

  /** Utility method that constructs a `ChangeMessageVisibilityBatchRequest` instance. */
  protected def newChangeMessageVisibilityBatchRequest(
    queueUrl: String,
    entries: Seq[(Message.Receipt[M], Int)] //
    ): ChangeMessageVisibilityBatchRequest =
    new ChangeMessageVisibilityBatchRequest()
      .withQueueUrl(queueUrl)
      .withEntries(entries.zipWithIndex.map {
        case ((receipt, visibilityTimeout), id) =>
          new ChangeMessageVisibilityBatchRequestEntry()
            .withId(id.toString)
            .withReceiptHandle(receipt.handle)
            .withVisibilityTimeout(visibilityTimeout)
      }.asJava)

  /** Utility method that extracts a `ChangeMessageVisibilityBatchResult` instance. */
  protected def changeMessageVisibilityBatchResultToMessages(
    result: ChangeMessageVisibilityBatchResult,
    entries: Seq[(Message.Receipt[M], Int)] //
    ): Seq[Message[M]] = {
    val messages = entries.toIndexedSeq
    val results = collection.mutable.IndexedSeq.fill[Message[M]](messages.length)(null)
    result.getSuccessful.asScala foreach { e =>
      val id = e.getId.toInt
      results(id) = Message.Changed(messages(id)._1.body)
    }
    result.getFailed.asScala foreach { e =>
      val id = e.getId.toInt
      results(id) = Message.Error(e.getCode, e.getMessage, e.getSenderFault, messages(id)._1.body)
    }
    results
  }

  /** Utility method that constructs a `DeleteMessageRequest` instance. */
  protected def newDeleteMessageRequest(queueUrl: String, receipt: Message.Receipt[M]): DeleteMessageRequest =
    new DeleteMessageRequest()
      .withQueueUrl(queueUrl)
      .withReceiptHandle(receipt.handle)

  /** Utility method that handles the result of a `DeleteMessageRequest` operation. */
  protected def deleteMessageResultToMessage(receipt: Message.Receipt[M]): Message.Deleted[M] =
    Message.Deleted(receipt.body)

  /** Utility method that constructs a `DeleteMessageBatchRequest` instance. */
  protected def newDeleteMessageBatchRequest(
    queueUrl: String,
    receipts: Seq[Message.Receipt[M]] //
    ): DeleteMessageBatchRequest =
    new DeleteMessageBatchRequest()
      .withQueueUrl(queueUrl)
      .withEntries(receipts.zipWithIndex.map {
        case (receipt, id) =>
          new DeleteMessageBatchRequestEntry()
            .withId(id.toString)
            .withReceiptHandle(receipt.handle)
      }.asJava)

  /** Utility method that extracts a `DeleteMessageBatchResult` instance. */
  protected def deleteMessageBatchResultToMessages(
    result: DeleteMessageBatchResult,
    receipts: Seq[Message.Receipt[M]] //
    ): Seq[Message[M]] = {
    val messages = receipts.toIndexedSeq
    val results = collection.mutable.IndexedSeq.fill[Message[M]](messages.length)(null)
    result.getSuccessful.asScala foreach { e =>
      val id = e.getId.toInt
      results(id) = Message.Deleted(messages(id).body)
    }
    result.getFailed.asScala foreach { e =>
      val id = e.getId.toInt
      results(id) = Message.Error(e.getCode, e.getMessage, e.getSenderFault, messages(id).body)
    }
    results
  }

  /**
   * Base class for type classes that identify entries in batch operations.
   */
  sealed trait BatchEntry[-E] extends (E => (M, Int))

  /**
   * Definitions of the supported batch entry type classes.
   */
  object BatchEntry {

    /**
     * Type class for batch entries with no initial delay.
     */
    implicit case object NoDelay extends BatchEntry[M] {
      override def apply(entry: M) = entry -> -1
    }

    /**
     * Type class for batch entries with an initial delay.
     */
    implicit case object WithDelay extends BatchEntry[(M, Int)] {
      override def apply(entry: (M, Int)) = entry
    }

  }

}

/**
 * Declarations of the attributes exposed by queues.
 */
object Queue extends Attributes {

  /** The queue-specific attribute key type. */
  override type Key[T] = KeyImpl[T]

  /** The queue-specific mutable attribute type. */
  type MutableAttribute[T] = (MutableKey[T], T)
  
  /** A default retry policy that never retries. */
  val defaultRetryPolicy = RetryPolicy(TerminationPolicy.LimitNumberOfAttempts(1))

  /** The queue-specific attribute keys. */
  override val keys = Seq(
    ApproximateNumberOfMessages,
    ApproximateNumberOfMessagesDelayed,
    ApproximateNumberOfMessagesNotVisible,
    CreatedTimestamp,
    DelaySeconds,
    LastModifiedTimestamp,
    MaximumMessageSize,
    MessageRetentionPeriod,
    Policy,
    QueueArn,
    ReceiveMessageWaitTimeSeconds,
    VisibilityTimeout)

  /**
   * A marker trait for keys of mutable attributes.
   *
   * @tparam T The type of value associated with this key.
   */
  sealed trait MutableKey[T] extends Key[T]

  /**
   * The queue-specific attribute key base class.
   *
   * @tparam T The type of value associated with this key.
   */
  sealed abstract class KeyImpl[T: Mapper](awsName: QueueAttributeName)
    extends KeySupport[T](awsName.name, implicitly[Mapper[T]])

  /**
   * The `ApproximateNumberOfMessages` attribute key.
   */
  case object ApproximateNumberOfMessages extends KeyImpl[Long](QueueAttributeName.ApproximateNumberOfMessages)

  /**
   * The `ApproximateNumberOfMessagesDelayed` attribute key.
   */
  case object ApproximateNumberOfMessagesDelayed
    extends KeyImpl[Long](QueueAttributeName.ApproximateNumberOfMessagesDelayed)

  /**
   * The `ApproximateNumberOfMessagesNotVisible` attribute key.
   */
  case object ApproximateNumberOfMessagesNotVisible
    extends KeyImpl[Long](QueueAttributeName.ApproximateNumberOfMessagesNotVisible)

  /**
   * The `CreatedTimestamp` attribute key.
   */
  case object CreatedTimestamp extends KeyImpl[Long](QueueAttributeName.CreatedTimestamp)

  /**
   * The `DelaySeconds` attribute key.
   */
  case object DelaySeconds extends KeyImpl[Int](QueueAttributeName.DelaySeconds) with MutableKey[Int]

  /**
   * The `LastModifiedTimestamp` attribute key.
   */
  case object LastModifiedTimestamp extends KeyImpl[Long](QueueAttributeName.LastModifiedTimestamp)

  /**
   * The `MaximumMessageSize` attribute key.
   */
  case object MaximumMessageSize extends KeyImpl[Int](QueueAttributeName.MaximumMessageSize) with MutableKey[Int]

  /**
   * The `MessageRetentionPeriod` attribute key.
   */
  case object MessageRetentionPeriod
    extends KeyImpl[Int](QueueAttributeName.MessageRetentionPeriod) with MutableKey[Int]

  /**
   * The `Policy` attribute key.
   */
  case object Policy extends KeyImpl[String](QueueAttributeName.Policy) with MutableKey[String]

  /**
   * The `QueueArn` attribute key.
   */
  case object QueueArn extends KeyImpl[String](QueueAttributeName.QueueArn)

  /**
   * The `ReceiveMessageWaitTimeSeconds` attribute key.
   */
  case object ReceiveMessageWaitTimeSeconds
    extends KeyImpl[Int](QueueAttributeName.ReceiveMessageWaitTimeSeconds) with MutableKey[Int]

  /**
   * The `VisibilityTimeout` attribute key.
   */
  case object VisibilityTimeout extends KeyImpl[Int](QueueAttributeName.VisibilityTimeout) with MutableKey[Int]

}