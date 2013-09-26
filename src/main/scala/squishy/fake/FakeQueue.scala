/* FakeQueue.scala
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

import collection.JavaConverters._
import java.util.concurrent.{
  Delayed,
  DelayQueue,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicLong
import java.security.MessageDigest

/**
 * A simple SQS queue implementation for testing purposes.
 */
class FakeQueue(val name: String) {

  import FakeQueue._

  /** The URL of this queue. */
  val url = "squishy:fake://" + name
  /** The ARN of this queue. */
  val arn = "arn:squishy:fake:" + name
  /** The Unix time in seconds this queue was created at. */
  val createdTimestamp = now
  /** The underlying delay queue. */
  private val queue = new DelayQueue[Message]
  /** Generator for unique message IDs. */
  private val idGenerator = new AtomicLong
  /** Generator for unique message receive receipts. */
  private val receiptGenerator = new AtomicLong
  /** The number of seconds to delay new messages. */
  @volatile
  private var _delaySeconds = 0L
  /** The number of seconds to wait for a message in receive. */
  @volatile
  private var _receiveMessageWaitTimeSeconds = 0L
  /** The number of seconds that received messages are kept from other consumers. */
  @volatile
  private var _visibilityTimeout = 30L
  /** The maximum number of seconds that a message is kept in the queue. */
  @volatile
  private var _messageRetentionPeriod = 345600L
  /** The maximum allowed size of messages in bytes. */
  @volatile
  private var _maximumMessageSize = 262144L
  /** The queue's security policy (currently not used). */
  @volatile
  private var _policy = ""
  /** The Unix time in seconds this queue was last modified at. */
  @volatile
  private var _lastModifiedTimestamp = createdTimestamp

  /** Returns the number of seconds to delay new messages. */
  def delaySeconds = _delaySeconds

  /** Sets the number of seconds to delay new messages. */
  def delaySeconds_=(delaySeconds: Long) {
    _delaySeconds = delaySeconds
    _lastModifiedTimestamp = now
  }

  /** Returns the number of seconds to wait for a message in receive. */
  def receiveMessageWaitTimeSeconds = _receiveMessageWaitTimeSeconds

  /** Sets the number of seconds to wait for a message in receive. */
  def receiveMessageWaitTimeSeconds_=(receiveMessageWaitTimeSeconds: Long) {
    _receiveMessageWaitTimeSeconds = receiveMessageWaitTimeSeconds
    _lastModifiedTimestamp = now
  }

  /** Returns the number of seconds that received messages are kept from other consumers. */
  def visibilityTimeout = _visibilityTimeout

  /** Sets the number of seconds that received messages are kept from other consumers. */
  def visibilityTimeout_=(visibilityTimeout: Long) {
    _visibilityTimeout = visibilityTimeout
    _lastModifiedTimestamp = now
  }

  /** Returns the maximum number of seconds that a message is kept in the queue. */
  def messageRetentionPeriod = _messageRetentionPeriod

  /** Sets the maximum number of seconds that a message is kept in the queue. */
  def messageRetentionPeriod_=(messageRetentionPeriod: Long) {
    _messageRetentionPeriod = messageRetentionPeriod
    _lastModifiedTimestamp = now
  }

  /** Returns the maximum allowed size of messages in bytes. */
  def maximumMessageSize = _maximumMessageSize

  /** Sets the maximum allowed size of messages in bytes. */
  def maximumMessageSize_=(maximumMessageSize: Long) {
    _maximumMessageSize = maximumMessageSize
    _lastModifiedTimestamp = now
  }

  /** Returns the queue's security policy (currently not used). */
  def policy = _policy

  /** Sets queue's security policy (currently not used). */
  def policy_=(policy: String) {
    _policy = policy
    _lastModifiedTimestamp = now
  }

  /** Returns the Unix time in seconds this queue was last modified at. */
  def lastModifiedTimestamp = _lastModifiedTimestamp

  /** Returns the number of messages in the queue. */
  def approximateNumberOfMessages = queue.size

  /** Returns the number of invisible messages in the queue. */
  def approximateNumberOfMessagesNotVisible =
    queue.asScala.count(msg => !msg.isVisible && msg.approximateReceiveCount > 0)

  /** Returns the number of delayed messages in the queue. */
  def approximateNumberOfMessagesDelayed =
    queue.asScala.count(msg => !msg.isVisible && msg.approximateReceiveCount == 0)

  /** Places a message in this queue if the content is not too large. */
  def send(message: String, delaySeconds: Option[Long]): Option[Message] = {
    if (message.getBytes("UTF-8").length > _maximumMessageSize)
      None
    else {
      val msg = new Message(
        idGenerator.incrementAndGet(),
        message,
        delaySeconds getOrElse _delaySeconds)
      queue.offer(msg)
      Some(msg)
    }
  }

  /** Attempts to receive a message from this queue. */
  def receive(
    maxMessages: Int,
    waitTimeSeconds: Option[Long],
    visibilityTimeout: Option[Long]): List[(Message, String)] = {
    val vt = visibilityTimeout getOrElse _visibilityTimeout

    def onReceive(msg: Message): Option[(Message, String)] =
      if (msg.ageInSeconds > _messageRetentionPeriod)
        None
      else {
        val receipt = receiptGenerator.incrementAndGet().toString
        msg.receive(vt, receipt)
        queue.offer(msg)
        Some(msg -> receipt)
      }

    val waitTime = waitTimeSeconds getOrElse _receiveMessageWaitTimeSeconds
    val startAt = now
    var timeout = waitTime
    var result = None: Option[(Message, String)]
    try {
      do {
        result = Option(queue.poll(timeout, TimeUnit.SECONDS)) flatMap onReceive
        if (result.isEmpty)
          timeout = waitTime - (now - startAt)
      } while (result.isEmpty && timeout > 0)
    } catch { case _: InterruptedException => }
    result map { firstMessage =>
      firstMessage :: Stream
        .continually(queue.poll())
        .takeWhile(_ != null)
        .flatMap(onReceive)
        .take(maxMessages - 1)
        .toList
    } getOrElse Nil
  }

  /** Attempts to change the visibility timeout of a message in this queue. */
  def changeVisibility(receipt: String, additionalSeconds: Long): Option[Message] =
    find(receipt) flatMap { msg =>
      if (!queue.remove(msg))
        None
      else if (!msg.isVisible && msg.changeVisibility(additionalSeconds)) {
        queue.offer(msg)
        Some(msg)
      } else {
        queue.offer(msg)
        None
      }
    }

  /** Deletes a message from this queue. */
  def delete(receipt: String): Option[Message] =
    find(receipt) flatMap { msg =>
      if (queue.remove(msg))
        Some(msg)
      else
        None
    }

  /** Looks up the message with the specified ID. */
  private def find(receipt: String): Option[Message] =
    queue.asScala find (_.checkReceipt(receipt))

  /** Returns a stream that continuously polls the queue without waiting. */
  private def pollContinuously: Stream[Message] =
    Stream continually queue.poll() takeWhile (_ != null)

}

/**
 * Definitions associated with the fake queue implementation.
 */
object FakeQueue {

  /** Returns the timestamp for the current Unix time in seconds. */
  private def now = System.currentTimeMillis / 1000L

  /**
   * A single fake SQS message.
   */
  class Message(val id: Long, val content: String, delayForSeconds: Long) extends Delayed {

    /** Stand-in for the sender ID. */
    val senderId = "fake"
    /** The MD5 hash of the message content. */
    val md5 = MessageDigest.getInstance("MD5").digest(content.getBytes("UTF-8")).mkString("")
    /** The timestamp of when the message was sent. */
    val sentTimestamp = now
    /** The point in time that this message becomes visible. */
    @volatile
    private var becomesVisibleAt = now + delayForSeconds
    /** The receipt attached to this message. */
    @volatile
    private var mostRecentReceipt = None: Option[String]
    /** The timestamp of the receipt attached to this message. */
    @volatile
    private var mostRecentReceiptTimestamp = None: Option[Long]
    /** The timestamp of the first receive attempt. */
    @volatile
    private var _approximateFirstReceiveTimestamp = None: Option[Long]
    /** The number of attempted receive operations. */
    @volatile
    private var _approximateReceiveCount = 0

    /** Returns the timestamp of the first receive attempt. */
    def approximateFirstReceiveTimestamp = _approximateFirstReceiveTimestamp

    /** Returns the number of attempted receive operations. */
    def approximateReceiveCount = _approximateReceiveCount

    /** Returns true if this message is currently visible. */
    private[FakeQueue] def isVisible = becomesVisibleAt <= now

    /** Returns the age of this message in seconds. */
    private[FakeQueue] def ageInSeconds = now - sentTimestamp

    /** Marks this message as held for the specified time period and assigns the receipt. */
    private[FakeQueue] def receive(visibilityTimeout: Long, reciept: String) {
      becomesVisibleAt = now + visibilityTimeout
      mostRecentReceipt = Some(reciept)
      mostRecentReceiptTimestamp = Some(System.currentTimeMillis)
      if (_approximateFirstReceiveTimestamp.isEmpty)
        _approximateFirstReceiveTimestamp = mostRecentReceiptTimestamp
      _approximateReceiveCount += 1
    }

    /** Returns the receipt attached to this message. */
    private[FakeQueue] def checkReceipt(reciept: String): Boolean = {
      mostRecentReceipt.map(_ == reciept).getOrElse(false)
    }

    /** Attempts to increase the visibility timeout of the current receive operation. */
    private[FakeQueue] def changeVisibility(additionalSeconds: Long): Boolean = {
      val willNowBecomeVisibleAt = becomesVisibleAt + additionalSeconds
      if (willNowBecomeVisibleAt > mostRecentReceiptTimestamp.get + TimeUnit.HOURS.toSeconds(12))
        false
      else {
        becomesVisibleAt = willNowBecomeVisibleAt
        true
      }
    }

    /** @inheritdoc */
    override def getDelay(unit: TimeUnit) =
      unit.convert(becomesVisibleAt - now, TimeUnit.SECONDS)

    /** @inheritdoc */
    override def compareTo(delayed: Delayed) = {
      val that = delayed.asInstanceOf[Message]
      if (this.becomesVisibleAt < that.becomesVisibleAt)
        -1
      else if (this.becomesVisibleAt > that.becomesVisibleAt)
        1
      else
        this.id compareTo that.id
    }

  }

}