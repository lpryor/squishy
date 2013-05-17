/* FakeSQSAsync.scala
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

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import java.util.concurrent.{
  Callable,
  Future,
  Executors,
  TimeUnit
}

/**
 * A simple implementation of the Amazon SQS asynchronous interface for testing purposes.
 */
class FakeSQSAsync extends FakeSQS with AmazonSQSAsync {

  /** The executor used to schedule requests. */
  private val executor = Executors.newCachedThreadPool()

  /** @inheritdoc */
  override def createQueueAsync(request: CreateQueueRequest) =
    submit(createQueue, request)

  /** @inheritdoc */
  override def createQueueAsync(
    request: CreateQueueRequest, handler: AsyncHandler[CreateQueueRequest, CreateQueueResult]) =
    submit(createQueue, request, handler)

  /** @inheritdoc */
  override def listQueuesAsync(request: ListQueuesRequest) =
    submit(listQueues, request)

  /** @inheritdoc */
  override def listQueuesAsync(request: ListQueuesRequest, handler: AsyncHandler[ListQueuesRequest, ListQueuesResult]) =
    submit(listQueues, request, handler)

  /** @inheritdoc */
  override def deleteQueueAsync(request: DeleteQueueRequest) =
    submitVoid(deleteQueue, request)

  /** @inheritdoc */
  override def deleteQueueAsync(request: DeleteQueueRequest, handler: AsyncHandler[DeleteQueueRequest, Void]) =
    submitVoid(deleteQueue, request, handler)

  /** @inheritdoc */
  override def getQueueUrlAsync(request: GetQueueUrlRequest) =
    submit(getQueueUrl, request)

  /** @inheritdoc */
  override def getQueueUrlAsync(
    request: GetQueueUrlRequest, handler: AsyncHandler[GetQueueUrlRequest, GetQueueUrlResult]) =
    submit(getQueueUrl, request, handler)

  /** @inheritdoc */
  override def getQueueAttributesAsync(request: GetQueueAttributesRequest) =
    submit(getQueueAttributes, request)

  /** @inheritdoc */
  override def getQueueAttributesAsync(
    request: GetQueueAttributesRequest, handler: AsyncHandler[GetQueueAttributesRequest, GetQueueAttributesResult]) =
    submit(getQueueAttributes, request, handler)

  /** @inheritdoc */
  override def setQueueAttributesAsync(request: SetQueueAttributesRequest) =
    submitVoid(setQueueAttributes, request)

  /** @inheritdoc */
  override def setQueueAttributesAsync(
    request: SetQueueAttributesRequest, handler: AsyncHandler[SetQueueAttributesRequest, Void]) =
    submitVoid(setQueueAttributes, request, handler)

  /** @inheritdoc */
  override def sendMessageAsync(request: SendMessageRequest) =
    submit(sendMessage, request)

  /** @inheritdoc */
  override def sendMessageAsync(
    request: SendMessageRequest, handler: AsyncHandler[SendMessageRequest, SendMessageResult]) =
    submit(sendMessage, request, handler)

  /** @inheritdoc */
  override def sendMessageBatchAsync(request: SendMessageBatchRequest) =
    submit(sendMessageBatch, request)

  /** @inheritdoc */
  override def sendMessageBatchAsync(
    request: SendMessageBatchRequest, handler: AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult]) =
    submit(sendMessageBatch, request, handler)

  /** @inheritdoc */
  override def receiveMessageAsync(request: ReceiveMessageRequest) =
    submit(receiveMessage, request)

  /** @inheritdoc */
  override def receiveMessageAsync(
    request: ReceiveMessageRequest, handler: AsyncHandler[ReceiveMessageRequest, ReceiveMessageResult]) =
    submit(receiveMessage, request, handler)

  /** @inheritdoc */
  override def changeMessageVisibilityAsync(request: ChangeMessageVisibilityRequest) =
    submitVoid(changeMessageVisibility, request)

  /** @inheritdoc */
  override def changeMessageVisibilityAsync(
    request: ChangeMessageVisibilityRequest, handler: AsyncHandler[ChangeMessageVisibilityRequest, Void]) =
    submitVoid(changeMessageVisibility, request, handler)

  /** @inheritdoc */
  override def changeMessageVisibilityBatchAsync(request: ChangeMessageVisibilityBatchRequest) =
    submit(changeMessageVisibilityBatch, request)

  /** @inheritdoc */
  override def changeMessageVisibilityBatchAsync(
    request: ChangeMessageVisibilityBatchRequest,
    handler: AsyncHandler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult]) =
    submit(changeMessageVisibilityBatch, request, handler)

  /** @inheritdoc */
  override def deleteMessageAsync(request: DeleteMessageRequest) =
    submitVoid(deleteMessage, request)

  /** @inheritdoc */
  override def deleteMessageAsync(request: DeleteMessageRequest, handler: AsyncHandler[DeleteMessageRequest, Void]) =
    submitVoid(deleteMessage, request, handler)

  /** @inheritdoc */
  override def deleteMessageBatchAsync(request: DeleteMessageBatchRequest) =
    submit(deleteMessageBatch, request)

  /** @inheritdoc */
  override def deleteMessageBatchAsync(
    request: DeleteMessageBatchRequest, handler: AsyncHandler[DeleteMessageBatchRequest, DeleteMessageBatchResult]) =
    submit(deleteMessageBatch, request, handler)

  /** @inheritdoc */
  override def addPermissionAsync(request: AddPermissionRequest) =
    submit(addPermission, request)

  /** @inheritdoc */
  override def addPermissionAsync(request: AddPermissionRequest, handler: AsyncHandler[AddPermissionRequest, Void]) =
    submit(addPermission, request, handler)

  /** @inheritdoc */
  override def removePermissionAsync(request: RemovePermissionRequest) =
    submit(removePermission, request)

  /** @inheritdoc */
  override def removePermissionAsync(
    request: RemovePermissionRequest, handler: AsyncHandler[RemovePermissionRequest, Void]) =
    submit(removePermission, request, handler)

  /** @inheritdoc */
  override protected def dispose() {
    super.dispose()
    executor.shutdown()
    try
      executor.awaitTermination(1L, TimeUnit.SECONDS)
    catch {
      case e: InterruptedException =>
    }
    if (!executor.isTerminated)
      executor.shutdownNow()
  }

  /** Submits a task to be executed in the future. */
  private def submit[I <: AmazonWebServiceRequest, O](f: I => O, i: I): Future[O] =
    executor.submit(new Callable[O] {
      override def call() = f(i)
    })

  /** Submits a task to be executed in the future. */
  private def submitVoid[I <: AmazonWebServiceRequest](f: I => Unit, i: I): Future[Void] =
    submit[I, Void](f andThen (_ => null), i)

  /** Submits a task to be executed in the future. */
  private def submit[I <: AmazonWebServiceRequest, O](f: I => O, i: I, h: AsyncHandler[I, O]): Future[O] =
    executor.submit(new Callable[O] {
      override def call() = {
        try {
          val o = f(i)
          h.onSuccess(i, o)
          o
        } catch {
          case e: Exception =>
            h.onError(e)
            throw e
        }
      }
    })

  /** Submits a task to be executed in the future. */
  private def submitVoid[I <: AmazonWebServiceRequest](f: I => Unit, i: I, h: AsyncHandler[I, Void]): Future[Void] =
    submit[I, Void](f andThen (_ => null), i, h)

}