/* Attributes.scala
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

import language.higherKinds

/**
 * A template that supports the definition of type safe key-value attributes.
 *
 * This class is used by both the [[squishy.Queue]] and [[squishy.Message]] classes to model attributes available from
 * the underlying SQS implementation.
 */
trait Attributes {

  /** Abstract type of valid attribute keys. */
  type Key[T] <: KeyApi[T]

  /** Alias to a typed key-value pair. */
  type Attribute[T] = (Key[T], T)

  /** All the valid keys for this attribute descriptor indexed by name. */
  private lazy val keyIndex: Map[String, Key[_]] = keys.map(k => k.name -> k).toMap

  /** Returns all the valid keys for this attribute descriptor. */
  def keys: Seq[Key[_]]

  /**
   * The base API required of `Key` implementations.
   *
   * @tparam T The type of value associated with this key.
   */
  trait KeyApi[T] { self: Key[T] =>

    /** Returns the name of this key. */
    def name: String

    /** Returns a mapper for the values of this key. */
    def mapper: Mapper[T]

  }

  /**
   * A support class for implementations of `KeyApi`.
   *
   * @tparam T The type of value associated with this key.
   */
  abstract class KeySupport[T](override val name: String, override val mapper: Mapper[T])
    extends KeyApi[T] { self: Key[T] => }

  /**
   * Utility class for dealing with collections of attributes in a type-safe manner.
   */
  final class AttributeSet(val entries: Seq[Attribute[_]]) {

    /** The attributes in this set indexed by key. */
    private lazy val entryIndex: Map[Key[_], _] = entries.toMap

    /** Returns the attribute with the specified key. */
    def apply[T](key: Key[T]): T = entryIndex(key).asInstanceOf[T]

    /** Returns the attribute with the specified key if it exists in this set. */
    def get[T](key: Key[T]): Option[T] = entryIndex.get(key).map(_.asInstanceOf[T])

  }

  /**
   * Utility for marshaling and unmarshaling attributes sets to and from maps of strings.
   */
  object AttributeSet {

    /** Converts a collection of entries into an attribute set. */
    def apply(entries: (String, String)*): AttributeSet =
      new AttributeSet(entries.map { case (k, v) => decode(keyIndex(k), v) })

    /** Converts a map of entries into an attribute set. */
    def apply(entries: collection.Map[String, String]): AttributeSet =
      apply(entries.toSeq: _*)

    /** Converts a collection of attributes into an entry map. */
    def unapply(attributes: Attribute[_]*): Option[Map[String, String]] =
      Some(attributes.map { case (k, v) => k.name -> k.mapper(v) }.toMap)

    /** Converts an attribute set into an entry map. */
    def unapply(attributes: AttributeSet): Option[Map[String, String]] =
      unapply(attributes.entries: _*)

    /** Decodes a value into the type specified by its key. */
    private def decode[T](key: Key[T], value: String): Attribute[T] =
      key -> key.mapper.unapply(value)

  }

}