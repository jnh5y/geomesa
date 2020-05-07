/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor

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
  def register[DS <: GeoMesaDataStore[DS]](scan: ManagedScan[DS]): ScheduledFuture[_] = {
    val timeout = math.max(1, scan.timeout.absolute - System.currentTimeMillis())
    executor.schedule(new QueryKiller(scan), timeout, TimeUnit.MILLISECONDS)
  }

  /**
    * Trait for classes to be managed for timeouts
    */
  trait ManagedScan[DS <: GeoMesaDataStore[DS]] {

    /**
     * The query plan being run
     *
     * @return
     */
    def plan: QueryPlan[DS]

    /**
     * The timeout
     *
     * @return
     */
    def timeout: Timeout

    /**
     * Forcibly terminate the scan
     */
    def terminate(): Unit
  }

  case class Timeout(relative: Long, absolute: Long)

  object Timeout {
    def apply(relative: Long): Timeout = Timeout(relative, System.currentTimeMillis() + relative)
  }

  private class QueryKiller[DS <: GeoMesaDataStore[DS]](scan: ManagedScan[DS]) extends Runnable {
    override def run(): Unit = {
      logger.warn(
        s"Stopping scan on schema '${scan.plan.filter.index.sft.getTypeName}' with filter " +
          s"'${filterToString(scan.plan.filter.filter)}' based on timeout of ${scan.timeout.relative}ms")
      try { scan.terminate() } catch {
        case NonFatal(e) => logger.warn("Error cancelling scan:", e)
      }
    }
  }
}
