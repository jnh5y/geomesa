/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.concurrent._

import com.google.protobuf.ByteString
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{AbstractManagedScan, LowLevelScanner, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter

private class CoprocessorBatchScan(
    connection: Connection,
    table: TableName,
    ranges: Seq[Scan],
    options: Map[String, String],
    threads: Int,
    rpcThreads: Int,
    buffer: Int
  ) extends AbstractBatchScan[Scan, Array[Byte]](ranges, threads, buffer, CoprocessorBatchScan.Sentinel) {

  protected val pool = new CachedThreadPool(rpcThreads)

  override protected def scan(range: Scan, out: BlockingQueue[Array[Byte]]): Unit = {
    WithClose(GeoMesaCoprocessor.execute(connection, table, range, options, pool)) { results =>
      results.foreach { r =>
        if (r.size() > 0) {
          out.put(r.toByteArray)
        }
      }
    }
  }

  override def close(): Unit = {
    try { super.close() } finally {
      pool.shutdownNow()
    }
  }
}

object CoprocessorBatchScan {

  private val Sentinel = Array.empty[Byte]
  private val BufferSize = HBaseSystemProperties.ScanBufferSize.toInt.get

  /**
   * Start a coprocessor batch scan
   *
   * @param connection connection
   * @param table table
   * @param ranges ranges to scan
   * @param options coprocessor configuration
   * @param rpcThreads size of thread pool used for hbase rpc calls, across all client scan threads
   * @return
   */
  def apply(
      plan: HBaseQueryPlan,
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      options: Map[String, String],
      rpcThreads: Int,
      timeout: Option[Timeout]): CloseableIterator[Array[Byte]] = {
    timeout match {
      case None => new CoprocessorBatchScan(connection, table, ranges, options, ranges.length, rpcThreads, BufferSize).start()
      case Some(t) => new ManagedCoprocessorBatchScan(plan, connection, table, ranges, options, ranges.length, rpcThreads, BufferSize, t).start()
    }
  }

  private class ManagedCoprocessorBatchScan(
      plan: HBaseQueryPlan,
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      options: Map[String, String],
      threads: Int,
      rpcThreads: Int,
      buffer: Int,
      timeout: Timeout
    ) extends CoprocessorBatchScan(connection, table, ranges, options, threads, rpcThreads, buffer) {

    override protected def scan(range: Scan, out: BlockingQueue[Array[Byte]]): Unit = {
      if (timeout.absolute < System.currentTimeMillis()) {
        val iter = new ManagedCoprocessorIterator(range)
        try {
          iter.foreach { r =>
            if (r.size() > 0) {
              out.put(r.toByteArray)
            }
          }
        } finally {
          iter.close()
        }
      }
    }

    class ManagedCoprocessorIterator(range: Scan)
        extends AbstractManagedScan(timeout, new CoprocessorScanner(connection, table, range, options, pool)) {
      override protected def typeName: String = plan.filter.index.sft.getTypeName
      override protected def filter: Option[Filter] = plan.filter.filter
    }
  }

  class CoprocessorScanner(
      connection: Connection,
      table: TableName,
      scan: Scan,
      options: Map[String, String],
      executor: ExecutorService) extends LowLevelScanner[ByteString] {
    override lazy val iterator: CloseableIterator[ByteString] =
      GeoMesaCoprocessor.execute(connection, table, scan, options, executor)
    override def close(): Unit = iterator.close()
  }
}
