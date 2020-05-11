/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.util.control.NonFatal

/**
 * Delegates an iterator and throws all exceptions throught the 'next' method to get around geotools
 * wrapping iterators that catch and suppress exceptions in hasNext
 *
 * @param delegate delegate iterator
 * @tparam T type bounds
 */
class ExceptionalIterator[T](delegate: Iterator[T]) extends Iterator[T] {

  private var suppressed: Throwable = _

  override def hasNext: Boolean = {
    try { delegate.hasNext } catch {
      case NonFatal(e) =>
        suppressed = e
        true
    }
  }

  override def next(): T = {
    if (suppressed != null) {
      throw suppressed
    } else {
      delegate.next()
    }
  }
}

object ExceptionalIterator {

  def apply[T](iterator: Iterator[T]): Iterator[T] = new ExceptionalIterator(iterator)

  def apply[T](iterator: CloseableIterator[T]): CloseableIterator[T] = new ExceptionalCloseableIterator(iterator)

  class ExceptionalCloseableIterator[T](delegate: CloseableIterator[T])
      extends ExceptionalIterator(delegate) with CloseableIterator[T] {
    override def close(): Unit = delegate.close()
  }
}