/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.concurrent._

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{AbstractManagedScan, LowLevelScanner, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.opengis.filter.Filter

private class HBaseBatchScan(connection: Connection, table: TableName, ranges: Seq[Scan], threads: Int, buffer: Int)
    extends AbstractBatchScan[Scan, Result](ranges, threads, buffer, HBaseBatchScan.Sentinel) {

  protected val htable: Table = connection.getTable(table, new CachedThreadPool(threads))

  override protected def scan(range: Scan, out: BlockingQueue[Result]): Unit = {
    val scan = htable.getScanner(range)
    try {
      var result = scan.next()
      while (result != null) {
        out.put(result)
        result = scan.next()
      }
    } finally {
      scan.close()
    }
  }

  override def close(): Unit = {
    try { super.close() } finally {
      htable.close()
    }
  }
}

object HBaseBatchScan {

  import scala.collection.JavaConverters._

  private val Sentinel = new Result
  private val BufferSize = HBaseSystemProperties.ScanBufferSize.toInt.get

  /**
   * Creates a batch scan with parallelism across the given scans
   *
   * @param connection connection
   * @param table table to scan
   * @param ranges ranges
   * @param threads number of concurrently running scans
   * @return
   */
  def apply(
      plan: HBaseQueryPlan,
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[Result] = {
    timeout match {
      case None => new HBaseBatchScan(connection, table, ranges, threads, BufferSize).start()
      case Some(t) => new ManagedHBaseBatchScan(plan, connection, table, ranges, threads, BufferSize, t).start()
    }
  }

  private class ManagedHBaseBatchScan(
      plan: HBaseQueryPlan,
      connection: Connection,
      table: TableName,
      ranges: Seq[Scan],
      threads: Int,
      buffer: Int,
      timeout: Timeout
    ) extends HBaseBatchScan(connection, table, ranges, threads, buffer) {

    override protected def scan(range: Scan, out: BlockingQueue[Result]): Unit = {
      if (timeout.absolute < System.currentTimeMillis()) {
        val iter = new ManagedScanIterator(range)
        try {
          while (iter.hasNext) {
            out.put(iter.next())
          }
        } finally {
          iter.close()
        }
      }
    }

    private class ManagedScanIterator(range: Scan)
        extends AbstractManagedScan(timeout, new HBaseScanner(htable.getScanner(range))) {
      override protected def typeName: String = plan.filter.index.sft.getTypeName
      override protected def filter: Option[Filter] = plan.filter.filter
    }
  }

  private class HBaseScanner(scanner: ResultScanner) extends LowLevelScanner[Result] {
    override def iterator: Iterator[Result] = scanner.iterator.asScala
    override def close(): Unit = scanner.close()
  }
}
