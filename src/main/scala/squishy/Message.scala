/* Message.scala
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

/**
 * Base type for objects that contain information about messages sent to and received from SQS queues.
 *
 * @tparam M The type of body contained by this message.
 */
sealed trait Message[+M] {

  /** The body of this message. */
  def body: M

}

/**
 * Definitions of the message attributes and implementations.
 */
object Message extends Attributes {

  /** The message-specific attribute key type. */
  override type Key[T] = KeyImpl[T]

  /** The message-specific attribute keys. */
  override val keys = Seq(
    SenderId,
    ApproximateFirstReceiveTimestamp,
    ApproximateReceiveCount,
    SentTimestamp
  )

  /**
   * Representation of a message that has been sent.
   *
   * @tparam M The type of body contained by this message.
   * @param id The ID of this message as returned from SQS.
   * @param digest The MD5 digest of the message as returned from SQS.
   * @param body The body of this message.
   */
  final case class Sent[+M](
    id: String,
    digest: String,
    override val body: M)
    extends Message[M]

  /**
   * Representation of a message that has been received.
   *
   * @tparam M The type of body contained by this message.
   * @param id The ID of this message as returned from SQS.
   * @param digest The MD5 digest of the message as returned from SQS.
   * @param handle The receipt handle of this message as returned from SQS.
   * @param attributes The attributes of this message as returned from SQS.
   * @param body The body of this message.
   */
  final case class Receipt[+M](
    id: String,
    digest: String,
    handle: String,
    attributes: AttributeSet,
    override val body: M)
    extends Message[M]

  /**
   * Representation of a message that had its visibility changed.
   *
   * @tparam M The type of body contained by this message.
   * @param body The body of this message.
   */
  final case class Changed[+M](override val body: M) extends Message[M]

  /**
   * Representation of a message that has been deleted.
   *
   * @tparam M The type of body contained by this message.
   * @param body The body of this message.
   */
  final case class Deleted[+M](override val body: M) extends Message[M]

  /**
   * Representation of a message operation that resulted in an error.
   *
   * @tparam M The type of body contained by this message.
   * @param errorCode The error code associated with this message.
   * @param errorMessage The error message associated with this message.
   * @param senderFault True if the error was caused by the sender.
   * @param body The body of this message.
   */
  final case class Error[+M](
    errorCode: String,
    errorMessage: String,
    senderFault: Boolean,
    override val body: M)
    extends Message[M]

  /**
   * The message-specific attribute key base class.
   *
   * @tparam T The type of value associated with this key.
   */
  sealed abstract class KeyImpl[T: Mapper](awsName: String)
    extends KeySupport[T](awsName, implicitly[Mapper[T]])

  /**
   * The `SenderId` attribute key.
   */
  case object SenderId extends KeyImpl[String]("SenderId")

  /**
   * The `ApproximateFirstReceiveTimestamp` attribute key.
   */
  case object ApproximateFirstReceiveTimestamp extends KeyImpl[Long]("ApproximateFirstReceiveTimestamp")

  /**
   * The `ApproximateReceiveCount` attribute key.
   */
  case object ApproximateReceiveCount extends KeyImpl[Int]("ApproximateReceiveCount")

  /**
   * The `SentTimestamp` attribute key.
   */
  case object SentTimestamp extends KeyImpl[Long]("SentTimestamp")

}