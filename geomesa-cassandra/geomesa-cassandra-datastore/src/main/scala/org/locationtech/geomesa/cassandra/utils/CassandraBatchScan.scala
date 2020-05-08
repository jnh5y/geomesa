/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.utils

import java.nio.ByteBuffer
import java.util.concurrent._

import com.datastax.driver.core._
import org.locationtech.geomesa.cassandra.data.CassandraQueryPlan
import org.locationtech.geomesa.index.utils.AbstractBatchScan
import org.locationtech.geomesa.index.utils.ThreadManagement.{AbstractManagedScan, LowLevelScanner, Timeout}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.filter.Filter

private class CassandraBatchScan(session: Session, ranges: Seq[Statement], threads: Int, buffer: Int)
    extends AbstractBatchScan[Statement, Row](ranges, threads, buffer, CassandraBatchScan.Sentinel) {

  override protected def scan(range: Statement, out: BlockingQueue[Row]): Unit = {
    val results = session.execute(range).iterator()
    while (results.hasNext) {
      out.put(results.next)
    }
  }
}

object CassandraBatchScan {

  import scala.collection.JavaConverters._

  private val Sentinel: Row = new AbstractGettableData(ProtocolVersion.NEWEST_SUPPORTED) with Row {
    override def getIndexOf(name: String): Int = -1
    override def getColumnDefinitions: ColumnDefinitions = null
    override def getToken(i: Int): Token = null
    override def getToken(name: String): Token = null
    override def getPartitionKeyToken: Token = null
    override def getType(i: Int): DataType = null
    override def getValue(i: Int): ByteBuffer = null
    override def getName(i: Int): String = null
    override def getCodecRegistry: CodecRegistry = null
  }

  def apply(
      plan: CassandraQueryPlan,
      session: Session,
      ranges: Seq[Statement],
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[Row] = {
    timeout match {
      case None => new CassandraBatchScan(session, ranges, threads, 100000).start()
      case Some(t) => new ManagedCassandraBatchScan(plan, session, ranges, threads, 100000, t).start()
    }
  }

  private class ManagedCassandraBatchScan(
      plan: CassandraQueryPlan,
      session: Session,
      ranges: Seq[Statement],
      threads: Int,
      buffer: Int,
      timeout: Timeout
    ) extends CassandraBatchScan(session, ranges, threads, buffer) {

    override protected def scan(range: Statement, out: BlockingQueue[Row]): Unit = {
      if (timeout.absolute > System.currentTimeMillis()) {
        val iter = new ManagedScanIterator(range)
        try {
          // since closing the managed scan doesn't stop the query, check the terminated flag explicitly
          while (!iter.isTerminated && iter.hasNext) {
            out.put(iter.next())
          }
        } finally {
          iter.close()
        }
      }
    }

    class ManagedScanIterator(range: Statement)
        extends AbstractManagedScan(timeout, new CassandraScanner(session, range)) {
      override protected def typeName: String = plan.filter.index.sft.getTypeName
      override protected def filter: Option[Filter] = plan.filter.filter
    }
  }

  class CassandraScanner(session: Session, range: Statement) extends LowLevelScanner[Row] {
    override def iterator: Iterator[Row] = session.execute(range).iterator().asScala
    override def close(): Unit = {} // no way to close a scan...
  }
}
