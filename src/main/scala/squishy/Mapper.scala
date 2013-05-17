/* Mapper.scala
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
 * A strategy for encoding and decoding objects of type `T`.
 *
 * Implementations of this interface are used for encoding and decoding message objects and attribute values.
 */
trait Mapper[T] {

  /** Encodes and returns the specified value. */
  def apply(value: T): String

  /** Decodes and returns the specified string. */
  def unapply(encoded: String): T

}

/**
 * Common mapper implementations.
 */
object Mapper {

  /**
   * A mapper for boolean values.
   */
  implicit object Booleans extends Mapper[Boolean] {
    override def apply(value: Boolean) = value.toString
    override def unapply(encoded: String) = encoded.toBoolean
  }

  /**
   * A mapper for byte values.
   */
  implicit object Bytes extends Mapper[Byte] {
    override def apply(value: Byte) = value.toString
    override def unapply(encoded: String) = encoded.toByte
  }

  /**
   * A mapper for short values.
   */
  implicit object Shorts extends Mapper[Short] {
    override def apply(value: Short) = value.toString
    override def unapply(encoded: String) = encoded.toShort
  }

  /**
   * A mapper for integer values.
   */
  implicit object Ints extends Mapper[Int] {
    override def apply(value: Int) = value.toString
    override def unapply(encoded: String) = encoded.toInt
  }

  /**
   * A mapper for float values.
   */
  implicit object Floats extends Mapper[Float] {
    override def apply(value: Float) = value.toString
    override def unapply(encoded: String) = encoded.toFloat
  }

  /**
   * A mapper for long values.
   */
  implicit object Longs extends Mapper[Long] {
    override def apply(value: Long) = value.toString
    override def unapply(encoded: String) = encoded.toLong
  }

  /**
   * A mapper for double values.
   */
  implicit object Doubles extends Mapper[Double] {
    override def apply(value: Double) = value.toString
    override def unapply(encoded: String) = encoded.toDouble
  }

  /**
   * A mapper for string values.
   */
  implicit object Strings extends Mapper[String] {
    override def apply(value: String) = value
    override def unapply(encoded: String) = encoded
  }

}