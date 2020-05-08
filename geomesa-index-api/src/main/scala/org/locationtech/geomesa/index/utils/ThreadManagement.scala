/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement extends LazyLogging {

  private val executor = {
    val ex = new ScheduledThreadPoolExecutor(2)
    ex.setRemoveOnCancelPolicy(true)
    ExitingExecutor(ex, force = true)
  }

  /**
   * Register a scan with the thread manager
   *
   * @param scan scan to terminate
   * @return
   */
  def register(scan: ManagedScan[_]): ScheduledFuture[_] = {
    val timeout = math.max(1, scan.timeout.absolute - System.currentTimeMillis())
    executor.schedule(new QueryKiller(scan), timeout, TimeUnit.MILLISECONDS)
  }

  /**
   * Trait for scans that are managed, i.e. tracked and terminated if they exceed a specified timeout
   *
   * @tparam T type
   */
  trait ManagedScan[T] extends CloseableIterator[T] {

    /**
     * Scan timeout
     *
     * @return
     */
    def timeout: Timeout

    /**
     * Forcibly terminate the scan
     */
    def terminate(): Unit

    /**
     * Was the scan terminated due to timeout
     *
     * @return
     */
    def isTerminated: Boolean
  }

  /**
   * Abstract base class for managed scans
   *
   * @param timeout timeout
   * @param underlying low-level scan to be stopped
   * @tparam T type
   */
  abstract class AbstractManagedScan[T](val timeout: Timeout, underlying: LowLevelScanner[T])
      extends ManagedScan[T] with LazyLogging {

    private val (terminated, iter, cancel) = {
      if (System.currentTimeMillis() > timeout.absolute) {
        (new AtomicBoolean(false), underlying.iterator, Some(ThreadManagement.register(this)))
      } else {
        (new AtomicBoolean(true), Iterator.empty, None)
      }
    }

    // used for log messages
    protected def typeName: String
    protected def filter: Option[Filter]

    override def hasNext: Boolean = iter.hasNext
    override def next(): T = iter.next()

    override def terminate(): Unit = {
      terminated.set(true)
      try {
        logger.warn(
          s"Stopping scan on schema '$typeName' with filter '${filterToString(filter)}' " +
              s"based on timeout of ${timeout.relative}ms")
        underlying.close()
      } catch {
        case NonFatal(e) => logger.warn("Error cancelling scan:", e)
      }
    }

    override def isTerminated: Boolean = terminated.get

    override def close(): Unit = {
      cancel.foreach(_.cancel(false))
      underlying.close()
      if (terminated.get) {
        throw new RuntimeException(s"Scan terminated due to timeout of ${timeout.relative}ms")
      }
    }
  }

  /**
   * Low level scanner that can be closed to terminate a scan
   *
   * @tparam T type
   */
  trait LowLevelScanner[T] extends Closeable {
    def iterator: Iterator[T]
  }

  /**
   * Timeout holder
   *
   * @param relative relative timeout, in millis
   * @param absolute absolute timeout, in system millis since epoch
   */
  case class Timeout(relative: Long, absolute: Long)

  object Timeout {
    def apply(relative: Long): Timeout = Timeout(relative, System.currentTimeMillis() + relative)
  }

  /**
   *
   * @param scan
   */
  private class QueryKiller(scan: ManagedScan[_]) extends Runnable {
    override def run(): Unit = scan.terminate()
  }
}
