/* AsyncQueue.scala
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

import concurrent.{ ExecutionContext, Future, Promise }
import language.implicitConversions

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._
import java.util.concurrent.{ Future => JFuture }

/**
 * A wrapper around an asynchronous SNS queue that supports idiomatic Scala.
 *
 * Note: implementations will transparently acquire and cache this queue's URL when required. See the note about queue
 * URLs in the class description of [[squishy.Queue]].
 *
 * @tparam M The type of message that can be sent to and received from this queue.
 */
trait AsyncQueue[M] extends Queue[M] {

  /** @inheritdoc. */
  override val sqsClient: AmazonSQSAsync

  /** The execution context to schedule asynchronous operations with. */
  implicit val executionContext: ExecutionContext

  /**
   * Returns true if this queue exists in the cloud.
   */
  def existsAsync: Future[Boolean] =
    queueUrlAsync map (_.isDefined)

  /**
   * Returns the URL of this queue if it exists.
   */
  def queueUrlAsync: Future[Option[String]] =
    cachedQueueUrl match {
      case Some(queueUrl) =>
        Future.successful(queueUrl)
      case None =>
        val request = newGetQueueUrlRequest()
        retryPolicy.retryAsync(s"Queue($queueName).queueUrlAsync") {
          call(sqsClient.getQueueUrlAsync, request) map (Some(_)) recover {
            case e: QueueDoesNotExistException => None
          }
        } map { result =>
          val queueUrl = Some(result map getQueueUrlResultToQueueUrl)
          synchronized(if (cachedQueueUrl.isEmpty) cachedQueueUrl = queueUrl)
          cachedQueueUrl.get
        }
    }

  /**
   * Returns all of the attributes of this queue if it exists.
   */
  def attributesAsync: Future[Queue.AttributeSet] =
    attributesAsync(Queue.keys: _*)

  /**
   * Returns the specified attributes of this queue if it exists.
   *
   * @param keys The keys that identify the attributes to return.
   */
  def attributesAsync(keys: Queue.Key[_]*): Future[Queue.AttributeSet] =
    queueUrlAsync flatMap {
      case Some(queueUrl) =>
        val request = newGetQueueAttributesRequest(queueUrl, keys: _*)
        retryPolicy.retryAsync(s"Queue($queueName).attributesAsync") {
          call(sqsClient.getQueueAttributesAsync, request)
        } map getQueueAttributesResultToAttributeSet
      case None =>
        Future.successful(Queue.AttributeSet())
    }

  /**
   * Sets the attributes of this queue, throwing an exception if it does not exist.
   *
   * @param attributes The attributes to configure for this queue.
   */
  def attributesAsync_=(attributes: Seq[Queue.MutableAttribute[_]]): Future[Unit] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newSetQueueAttributesRequest(queueUrl, attributes)
      retryPolicy.retryAsync(s"Queue($queueName).attributesAsync = ...") {
        call(sqsClient.setQueueAttributesAsync, request)
      } map (_ => ())
    }

  /**
   * Creates this queue in the cloud if it does not already exist.
   *
   * All unspecified attributes will default to the values specified by Amazon SQS.
   *
   * @param attributes The attributes to configure for this queue.
   */
  def createQueueAsync(attributes: Queue.MutableAttribute[_]*): Future[Unit] = {
    val request = newCreateQueueRequest(attributes)
    retryPolicy.retryAsync(s"Queue($queueName).createQueueAsync") {
      call(sqsClient.createQueueAsync, request)
    } map { result =>
      val queueUrl = Some(Some(createQueueResultToQueueUrl(result)))
      synchronized(cachedQueueUrl = queueUrl)
    }
  }

  /**
   * Deletes this queue in the cloud if it exists.
   */
  def deleteQueueAsync_!(): Future[Unit] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newDeleteQueueRequest(queueUrl)
      retryPolicy.retryAsync(s"Queue($queueName).deleteQueueAsync_!") {
        call(sqsClient.deleteQueueAsync, request)
      } map { result =>
        val queueUrl = Some(None)
        synchronized(cachedQueueUrl = queueUrl)
      }
    }

  /**
   * Sends a message to this queue.
   *
   * All optional parameters of this method will default to the values specified by Amazon SQS.
   *
   * @param msg The body of the message to send.
   * @param delaySeconds The number of seconds to delay message availability.
   */
  def sendAsync(msg: M, delaySeconds: Int = -1): Future[Message.Sent[M]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newSendMessageRequest(queueUrl, msg, delaySeconds)
      retryPolicy.retryAsync(s"Queue($queueName).sendAsync") {
        call(sqsClient.sendMessageAsync, request)
      } map (sendMessageResultToMessage(_, msg))
    }

  /**
   * Sends a batch of messages to this queue.
   *
   * All optional parameters of this method will default to the values specified by Amazon SQS.
   *
   * @param entries The entries representing the messages to send. These must be of type `M` for immediate messages or
   * `(M, Int)` for messages with an initial delay.
   */
  def sendBatchAsync[E: BatchEntry](entries: E*): Future[Seq[Message[M]]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val entry = implicitly[BatchEntry[E]]
      val messages = entries map entry
      val request = newSendMessageBatchRequest(queueUrl, messages)
      retryPolicy.retryAsync(s"Queue($queueName).sendBatchAsync") {
        call(sqsClient.sendMessageBatchAsync, request)
      } map (sendMessageBatchResultToMessages(_, messages))
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
  def receiveAsync(
    maxNumberOfMessages: Int = -1,
    visibilityTimeout: Int = -1,
    waitTimeSeconds: Int = -1,
    attributes: Seq[Message.Key[_]] = Seq.empty //
    ): Future[Seq[Message.Receipt[M]]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newReceiveMessageRequest(
        queueUrl,
        maxNumberOfMessages,
        visibilityTimeout,
        waitTimeSeconds,
        attributes)
      retryPolicy.retryAsync(s"Queue($queueName).receiveAsync") {
        call(sqsClient.receiveMessageAsync, request)
      } map receiveMessageResultToMessages
    }

  /**
   * Attempts to extend the time that a message is invisible to other consumers.
   *
   * @param receipt The receipt of the message to modify the visibility of.
   * @param visibilityTimeout The number of seconds to extends the message's visibility timeout.
   */
  def changeVisibilityAsync(receipt: Message.Receipt[M], visibilityTimeout: Int): Future[Message.Changed[M]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newChangeMessageVisibilityRequest(queueUrl, receipt, visibilityTimeout)
      retryPolicy.retryAsync(s"Queue($queueName).changeVisibilityAsync") {
        call(sqsClient.changeMessageVisibilityAsync, request)
      } map (_ => changeMessageVisibilityResultToMessage(receipt))
    }

  /**
   * Attempts to extend the time that a batch of messages are invisible to other consumers.
   *
   * @param entries The entries representing the messages to change the visibility of with their new visibility timeout.
   */
  def changeVisibilityBatchAsync(entries: (Message.Receipt[M], Int)*): Future[Seq[Message[M]]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newChangeMessageVisibilityBatchRequest(queueUrl, entries)
      retryPolicy.retryAsync(s"Queue($queueName).changeVisibilityBatchAsync") {
        call(sqsClient.changeMessageVisibilityBatchAsync, request)
      } map (changeMessageVisibilityBatchResultToMessages(_, entries))
    }

  /**
   * Attempts to delete a message from this queue.
   *
   * @param receipt The receipt of the message to delete from the queue.
   */
  def deleteAsync(receipt: Message.Receipt[M]): Future[Message.Deleted[M]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newDeleteMessageRequest(queueUrl, receipt)
      retryPolicy.retryAsync(s"Queue($queueName).deleteAsync") {
        call(sqsClient.deleteMessageAsync, request)
      } map (_ => deleteMessageResultToMessage(receipt))
    }

  /**
   * Attempts to delete a batch of messages from this queue.
   *
   * @param receipts The receipts of the messages to delete from the queue.
   */
  def deleteBatchAsync(receipts: Message.Receipt[M]*): Future[Seq[Message[M]]] =
    requireQueueUrlAsync flatMap { queueUrl =>
      val request = newDeleteMessageBatchRequest(queueUrl, receipts)
      retryPolicy.retryAsync(s"Queue($queueName).deleteBatchAsync") {
        call(sqsClient.deleteMessageBatchAsync, request)
      } map (deleteMessageBatchResultToMessages(_, receipts))
    }

  /** Applies a function with the queue URL or signals an exception if it does not exist. */
  private def requireQueueUrlAsync: Future[String] =
    queueUrlAsync flatMap {
      case Some(url) => Future.successful(url)
      case None => Future.failed(new QueueDoesNotExistException(s"""Queue "$queueName" does not exist."""))
    }

  /** Invokes an AWS async method and returns a Scala future. */
  private def call[I <: AmazonWebServiceRequest, O](f: (I, AsyncHandler[I, O]) => JFuture[O], i: I): Future[O] = {
    val outcome = Promise[O]()
    f(i, new AsyncHandler[I, O] {
      override def onSuccess(request: I, result: O) = outcome.success(result)
      override def onError(thrown: Exception) = outcome.failure(thrown)
    })
    outcome.future
  }

}