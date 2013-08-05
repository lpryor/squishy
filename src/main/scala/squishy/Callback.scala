/* Callback.scala
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

import language.implicitConversions

/**
 * Base type for objects that process the outcome of an asynchronous operation.
 *
 * @tparam T The type of result expected on a successful outcome.
 */
trait Callback[-T] {

  /** Called when a successful outcome is detected. */
  def onSuccess(result: T)

  /** Called when an unsuccessful outcome is detected. */
  def onError(thrown: Exception)

  /** Creates a new callback that notifies this callback after transforming a successful result. */
  def map[U](f: U => T): Callback[U] = new Callback[U] {

    /** @inheritdoc */
    override def onSuccess(result: U) {
      Callback.this.onSuccess(f(result))
    }

    /** @inheritdoc */
    override def onError(thrown: Exception) {
      Callback.this.onError(thrown)
    }

  }

}

/**
 * Common callback implementations.
 */
object Callback {

  /** Implicitly converts a callback into an AWS `AsyncHandler`. */
  implicit def callbackToAsyncHandler[I <: AmazonWebServiceRequest, O](callback: Callback[O]): AsyncHandler[I, O] =
    new AsyncHandler[I, O] {
      override def onSuccess(request: I, result: O) = callback.onSuccess(result)
      override def onError(thrown: Exception) = callback.onError(thrown)
    }

  /** A callback that does nothing. */
  private val _empty = new Callback[Any] {

    /** @inheritdoc */
    override def onSuccess(result: Any) {}

    /** @inheritdoc */
    override def onError(thrown: Exception) {}

  }

  /** Returns a callback that does nothing. */
  def empty[T]: Callback[T] = _empty

  /**
   * Creates a callback that invokes the specified function on either success or failure, passing in an accessor that
   * will either return the successful result or throw the error exception.
   *
   * An example of creating a callback with this method:
   * {{{
   * val callback = Callback[String] { result =>
   *   try {
   *     doSomethingWith(result())
   *   } catch {
   *     case e: Exception =>
   *       log("Something has gone wrong: " + e.getMessage)
   *   }
   * }
   * }}}
   */
  def apply[T](f: (() => T) => Unit): Callback[T] = new Callback[T] {

    /** @inheritdoc */
    override def onSuccess(result: T) {
      f(() => result)
    }

    /** @inheritdoc */
    override def onError(thrown: Exception) {
      f(() => throw thrown)
    }

  }

  /**
   * Creates a callback that invokes the specified function on either success or failure, passing in an `Option` that
   * will either contain the successful result or be `None`.
   *
   * An example of creating a callback with this method:
   * {{{
   * val callback = Callback.opt[String] { result =>
   *   if (result.isDefined)
   *     doSomethingWith(result.get)
   *   else
   *     log("Something has gone wrong!")
   * }
   * }}}
   */
  def opt[T](f: Option[T] => Unit): Callback[T] = new Callback[T] {

    /** @inheritdoc */
    override def onSuccess(result: T) {
      f(Some(result))
    }

    /** @inheritdoc */
    override def onError(thrown: Exception) {
      f(None)
    }

  }

  /**
   * Creates a callback that invokes the specified function on either success or failure, passing in an `Either` that
   * will be either a `Left` containing the successful result or a `Right` containing the error exception.
   *
   * An example of creating a callback with this method:
   * {{{
   * val callback = Callback.either[String] { result =>
   *   result match {
   *     case Left(value) =>
   *       doSomethingWith(value)
   *     case Right(e) =>
   *       log("Something has gone wrong: " + e.getMessage)
   *   }
   * }
   * }}}
   */
  def either[T](f: Either[T, Exception] => Unit): Callback[T] = new Callback[T] {

    /** @inheritdoc */
    override def onSuccess(result: T) {
      f(Left(result))
    }

    /** @inheritdoc */
    override def onError(thrown: Exception) {
      f(Right(thrown))
    }

  }

  /**
   * Creates a callback that invokes the `fs` function on success or the `fe` function on failure.
   *
   * An example of creating a callback with this method:
   * {{{
   * val callback = Callback.fold[String](
   *   v => doSomethingWith(v),
   *   e => log("Something has gone wrong: " + e.getMessage)
   * )
   * }}}
   */
  def fold[T](fs: T => Unit, fe: Exception => Unit): Callback[T] = new Callback[T] {

    /** @inheritdoc */
    override def onSuccess(result: T) {
      fs(result)
    }

    /** @inheritdoc */
    override def onError(thrown: Exception) {
      fe(thrown)
    }

  }

}