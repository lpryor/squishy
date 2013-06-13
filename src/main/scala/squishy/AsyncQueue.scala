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

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import parallel.Future

import java.util.concurrent.CountDownLatch

/**
 * A wrapper around an asynchronous SNS queue that supports idiomatic Scala.
 *
 * Note: implementations will transparently acquire and cache this queue's URL when required. See the note about queue
 * URLs in the class description of [[squishy.Queue]].
 *
 * @tparam M The type of message that can be sent to and received from this queue.
 */
trait AsyncQueue[M] extends Queue[M] {

  import AsyncQueue._

  /** @inheritdoc. */
  override val sqsClient: AmazonSQSAsync

  /**
   * Returns true if this queue exists in the cloud.
   *
   * @param callback The callback that should be notified when the operation completes.
   */
  def existsAsync(callback: Callback[Boolean] = Callback.empty): Future[Boolean] = {
    val result = new FutureResult(callback)
    queueUrlAsync(result map (_.isDefined))
    result
  }

  /**
   * Returns the URL of this queue if it exists.
   *
   * @param callback The callback that should be notified when the operation completes.
   */
  def queueUrlAsync(callback: Callback[Option[String]] = Callback.empty): Future[Option[String]] = {
    val result = new FutureResult(callback)
    cachedQueueUrl match {
      case Some(queueUrl) =>
        result.onSuccess(queueUrl)
      case None =>
        val request = getQueueUrlRequest()
        retryPolicy.retryAsync("Queue(%s).queueUrlAsync" format queueName,
          result map { (queueUrl: Option[String]) =>
            synchronized {
              if (cachedQueueUrl.isEmpty)
                cachedQueueUrl = Some(queueUrl)
            }
            cachedQueueUrl.get
          }) { callback =>
            sqsClient.getQueueUrlAsync(request, new AsyncHandler[GetQueueUrlRequest, GetQueueUrlResult] {
              override def onSuccess(request: GetQueueUrlRequest, result: GetQueueUrlResult) =
                callback.onSuccess(Some(getQueueUrlResult(result)))
              override def onError(thrown: Exception) = thrown match {
                case e: QueueDoesNotExistException =>
                  callback.onSuccess(None)
                case e =>
                  callback.onError(e)
              }
            })
          }
    }
    result
  }

  /**
   * Returns the specified attributes of this queue if it exists.
   *
   * @param keys The keys that identify the attributes to return.
   * @param callback The callback that should be notified when the operation completes.
   */
  def attributesAsync(
    keys: Seq[Queue.Key[_]] = Seq.empty,
    callback: Callback[Queue.AttributeSet] = Callback.empty //
    ): Future[Queue.AttributeSet] = {
    val result = new FutureResult(callback)
    queueUrlAsync(new Callback[Option[String]] {
      override def onSuccess(queueUrl: Option[String]) {
        queueUrl match {
          case Some(queueUrl) =>
            val request = getQueueAttributesRequest(queueUrl, keys: _*)
            retryPolicy.retryAsync("Queue(%s).attributesAsync" format queueName, result) { callback =>
              sqsClient.getQueueAttributesAsync(request, callback map getQueueAttributesResult)
            }
          case None =>
            result.onSuccess(Queue.AttributeSet())
        }
      }
      override def onError(thrown: Exception) {
        result.onError(thrown)
      }
    })
    result
  }

  /**
   * Sets the attributes of this queue, throwing an exception if it does not exist.
   *
   * @param attributes The attributes to configure for this queue.
   * @param callback The callback that should be notified when the operation completes.
   */
  def setAttributesAsync(
    attributes: Seq[Queue.MutableAttribute[_]],
    callback: Callback[Unit] = Callback.empty //
    ): Future[Unit] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = setQueueAttributesRequest(queueUrl, attributes)
      retryPolicy.retryAsync("Queue(%s).setAttributesAsync" format queueName, result) { callback =>
        sqsClient.setQueueAttributesAsync(request, callback map ((result: Void) => ()))
      }
    }
    result
  }

  /**
   * Creates this queue in the cloud if it does not already exist.
   *
   * All unspecified attributes will default to the values specified by Amazon SQS.
   *
   * @param attributes The attributes to configure for this queue.
   * @param callback The callback that should be notified when the operation completes.
   */
  def createQueueAsync(
    attributes: Seq[Queue.MutableAttribute[_]] = Seq.empty,
    callback: Callback[Unit] = Callback.empty //
    ): Future[Unit] = {
    val result = new FutureResult(callback)
    val request = createQueueRequest(attributes)
    retryPolicy.retryAsync("Queue(%s).createQueueAsync" format queueName, result) { callback =>
      sqsClient.createQueueAsync(request, callback map { (result: CreateQueueResult) =>
        synchronized {
          cachedQueueUrl = Some(Some(createQueueResult(result)))
        }
      })
    }
    result
  }

  /**
   * Deletes this queue in the cloud if it exists.
   *
   * @param callback The callback that should be notified when the operation completes.
   */
  def deleteQueueAsync_!(callback: Callback[Unit] = Callback.empty): Future[Unit] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = deleteQueueRequest(queueUrl)
      retryPolicy.retryAsync("Queue(%s).deleteQueueAsync_!" format queueName, result) { callback =>
        sqsClient.deleteQueueAsync(request, callback map { (result: Void) =>
          synchronized {
            cachedQueueUrl = Some(None)
          }
        })
      }
    }
    result
  }

  /**
   * Sends a message to this queue.
   *
   * All optional parameters of this method, with the exception of the `callback` parameter, will default to the values
   * specified by Amazon SQS.
   *
   * @param msg The body of the message to send.
   * @param delaySeconds The number of seconds to delay message availability.
   * @param callback The callback that should be notified when the operation completes.
   */
  def sendAsync(
    msg: M,
    delaySeconds: Int = -1,
    callback: Callback[Message.Sent[M]] = Callback.empty //
    ): Future[Message.Sent[M]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = sendMessageRequest(queueUrl, msg, delaySeconds)
      retryPolicy.retryAsync("Queue(%s).sendAsync" format queueName, result) { callback =>
        sqsClient.sendMessageAsync(request, callback map ((r: SendMessageResult) => sendMessageResult(r, msg)))
      }
    }
    result
  }

  /**
   * Sends a batch of messages to this queue.
   *
   * All optional parameters of this method, with the exception of the `callback` parameter, will default to the values
   * specified by Amazon SQS.
   *
   * @param entries The entries representing the messages to send. These must be of type `M` for immediate messages or
   * `(M, Int)` for messages with an initial delay.
   * @param callback The callback that should be notified when the operation completes.
   */
  def sendBatchAsync[E: BatchEntry](
    entries: Seq[E],
    callback: Callback[Seq[Message[M]]] = Callback.empty //
    ): Future[Seq[Message[M]]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val typeCls = implicitly[BatchEntry[E]]
      val messages = entries map (e => (typeCls.body(e), typeCls.delaySeconds(e)))
      val request = sendMessageBatchRequest(queueUrl, messages)
      retryPolicy.retryAsync("Queue(%s).sendBatchAsync" format queueName, result) { callback =>
        sqsClient.sendMessageBatchAsync(request,
          callback map ((r: SendMessageBatchResult) => sendMessageBatchResult(r, messages)))
      }
    }
    result
  }

  /**
   * Attempts to receive one or more messages from this queue.
   *
   * All optional parameters of this method, with the exception of the `callback` parameter, will default to the values
   * specified by Amazon SQS.
   *
   * @param maxNumberOfMessages The maximum number of messages to receive.
   * @param visibilityTimeout The number of seconds to prevent other consumers from seeing received messages.
   * @param waitTimeSeconds The maximum number of seconds to wait for a message.
   * @param attributes The keys of the message attributes that should be returned along with the messages.
   * @param callback The callback that should be notified when the operation completes.
   */
  def receiveAsync(
    maxNumberOfMessages: Int = -1,
    visibilityTimeout: Int = -1,
    waitTimeSeconds: Int = -1,
    attributes: Seq[Message.Key[_]] = Seq.empty,
    callback: Callback[Seq[Message.Receipt[M]]] = Callback.empty //
    ): Future[Seq[Message.Receipt[M]]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = receiveMessageRequest(
        queueUrl,
        maxNumberOfMessages,
        visibilityTimeout,
        waitTimeSeconds,
        attributes)
      retryPolicy.retryAsync("Queue(%s).receiveAsync" format queueName, result) { callback =>
        sqsClient.receiveMessageAsync(request, callback map receiveMessageResult)
      }
    }
    result
  }

  /**
   * Attempts to extend the time that a message is invisible to other consumers.
   *
   * @param receipt The receipt of the message to modify the visibility of.
   * @param visibilityTimeout The number of seconds to extends the message's visibility timeout.
   * @param callback The callback that should be notified when the operation completes.
   */
  def changeVisibilityAsync(
    receipt: Message.Receipt[M],
    visibilityTimeout: Int,
    callback: Callback[Message.Changed[M]] = Callback.empty //
    ): Future[Message.Changed[M]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = changeMessageVisibilityRequest(queueUrl, receipt, visibilityTimeout)
      retryPolicy.retryAsync("Queue(%s).changeVisibilityAsync" format queueName, result) { callback =>
        sqsClient.changeMessageVisibilityAsync(request,
          callback map ((result: Void) => changeMessageVisibilityResult(receipt)))
      }
    }
    result
  }

  /**
   * Attempts to extend the time that a batch of messages are invisible to other consumers.
   *
   * @param entries The entries representing the messages to change the visibility of with their new visibility timeout.
   * @param callback The callback that should be notified when the operation completes.
   */
  def changeVisibilityBatchAsync(
    entries: Seq[(Message.Receipt[M], Int)],
    callback: Callback[Seq[Message[M]]] = Callback.empty //
    ): Future[Seq[Message[M]]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = changeMessageVisibilityBatchRequest(queueUrl, entries)
      retryPolicy.retryAsync("Queue(%s).changeVisibilityBatchAsync" format queueName, result) { callback =>
        sqsClient.changeMessageVisibilityBatchAsync(request,
          callback map ((result: ChangeMessageVisibilityBatchResult) =>
            changeMessageVisibilityBatchResult(result, entries)))
      }
    }
    result
  }

  /**
   * Attempts to delete a message from this queue.
   *
   * @param receipt The receipt of the message to delete from the queue.
   * @param callback The callback that should be notified when the operation completes.
   */
  def deleteAsync(
    receipt: Message.Receipt[M],
    callback: Callback[Message.Deleted[M]] = Callback.empty //
    ): Future[Message.Deleted[M]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = deleteMessageRequest(queueUrl, receipt)
      retryPolicy.retryAsync("Queue(%s).deleteAsync" format queueName, result) { callback =>
        sqsClient.deleteMessageAsync(request, callback map ((result: Void) => deleteMessageResult(receipt)))
      }
    }
    result
  }

  /**
   * Attempts to delete a batch of messages from this queue.
   *
   * @param receipts The receipts of the messages to delete from the queue.
   * @param callback The callback that should be notified when the operation completes.
   */
  def deleteBatchAsync(
    receipts: Seq[Message.Receipt[M]],
    callback: Callback[Seq[Message[M]]] = Callback.empty //
    ): Future[Seq[Message[M]]] = {
    val result = new FutureResult(callback)
    requireQueueUrl(result) { queueUrl =>
      val request = deleteMessageBatchRequest(queueUrl, receipts)
      retryPolicy.retryAsync("Queue(%s).deleteBatchAsync" format queueName, result) { callback =>
        sqsClient.deleteMessageBatchAsync(request,
          callback map ((result: DeleteMessageBatchResult) => deleteMessageBatchResult(result, receipts)))
      }
    }
    result
  }

  /** Applies a function with the queue URL or signals an exception if it does not exist. */
  private def requireQueueUrl(callback: Callback[_])(f: String => Unit) {
    queueUrlAsync(new Callback[Option[String]] {
      override def onSuccess(queueUrl: Option[String]) = queueUrl match {
        case Some(url) => f(url)
        case None => callback.onError(new IllegalStateException("Queue %s does not exist." format queueName))
      }
      override def onError(thrown: Exception) = callback.onError(thrown)
    })
  }

}

/**
 * Definitions associated with asynchronous queues.
 */
object AsyncQueue {

  /**
   * Implementation of the `Future` trait that participates in the callback chain.
   */
  private final class FutureResult[T](callback: Callback[T]) extends Future[T] with Callback[T] {

    /** The latch that tracks completion. */
    private val latch = new CountDownLatch(1)
    /** The outcome of the operation. */
    @volatile
    private var outcome: Either[T, Exception] = null

    /** @inheritdoc */
    override def isDone() =
      latch.getCount == 0L

    /** @inheritdoc */
    override def apply() = {
      latch.await()
      outcome.fold(identity, throw _)
    }

    /** @inheritdoc */
    override def onSuccess(result: T) {
      outcome = Left(result)
      latch.countDown()
      callback.onSuccess(result)
    }

    /** @inheritdoc */
    override def onError(thrown: Exception) {
      outcome = Right(thrown)
      latch.countDown()
      callback.onError(thrown)
    }

  }

}