/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import com.google.common.collect.Lists
import com.google.protobuf.ByteString
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HBaseFilter}
import org.geotools.factory.Hints
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.client.FilterAggregatingClient
import org.locationtech.geomesa.hbase.filters.KryoLazyDensityFilter
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.hbase.{HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap}
import org.opengis.feature.simple.SimpleFeature


sealed trait HBaseQueryPlan extends HBaseQueryPlanType {
  def filter: HBaseFilterStrategyType
  def table: TableName
  def ranges: Seq[Query]
  def remoteFilters: Seq[HBaseFilter]
  // note: entriesToFeatures encapsulates ecql and transform
  def resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]

  override def explain(explainer: Explainer, prefix: String): Unit =
    HBaseQueryPlan.explain(this, explainer, prefix)
}

object HBaseQueryPlan {

  def explain(plan: HBaseQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${Option(plan.table).orNull}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Remote Filters (${plan.remoteFilters.size}):", plan.remoteFilters.map(_.toString))
    explainer.popLevel()
  }

  private def rangeToString(range: Query): String = {
    range match {
      case r: Scan => s"[${r.getStartRow.mkString("")},${r.getStopRow.mkString("")}]"
      case r: Get => s"[${r.getRow.mkString("")},${r.getRow.mkString("")}]"
    }
  }
}

// plan that will not actually scan anything
case class EmptyPlan(filter: HBaseFilterStrategyType) extends HBaseQueryPlan {
  override val table: TableName = null
  override val ranges: Seq[Query] = Seq.empty
  override val remoteFilters: Seq[HBaseFilter] = Nil
  override val resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature] = (i) => Iterator.empty
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

case class ScanPlan(filter: HBaseFilterStrategyType,
                    table: TableName,
                    ranges: Seq[Scan],
                    remoteFilters: Seq[HBaseFilter] = Nil,
                    resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    ranges.foreach(ds.applySecurity)
    val results: HBaseBatchScan = new HBaseBatchScan(ds.connection, table, ranges, ds.config.queryThreads, 100000, remoteFilters)
    SelfClosingIterator(resultsToFeatures(results), results.close)
  }
}

case class DensityCoprocessorPlan(filter: HBaseFilterStrategyType,
                                  hints: Hints,
                                  table: TableName,
                                  ranges: Seq[Scan],
                                  remoteFilters: Seq[HBaseFilter] = Nil,
                                  resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {
  /**
    * Runs the query plain against the underlying database, returning the raw entries
    *
    * @param ds data store - provides connection object and metadata
    * @return
    */
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    val TEST_FAMILY = "an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326"
    var filter = new KryoLazyDensityFilter(TEST_FAMILY, hints)
    var arr = filter.toByteArray
    val table1 = ds.connection.getTable(table)

    import scala.collection.JavaConverters._
    val client = new FilterAggregatingClient()
    val result : List[ByteString] = client.kryoLazyDensityFilter(table1, arr).asScala.toList
    import org.locationtech.geomesa.hbase.utils.KryoLazyDensityFilterUtils._

    result.map (r => bytesToFeatures(r.toByteArray)).toIterator
  }
}

case class GetPlan(filter: HBaseFilterStrategyType,
                   table: TableName,
                   ranges: Seq[Get],
                   remoteFilters: Seq[HBaseFilter] = Nil,
                   resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._
    val filterList = new FilterList()
    remoteFilters.foreach { filter => filterList.addFilter(filter) }
    ranges.foreach { range =>
      range.setFilter(filterList)
      ds.applySecurity(range)
    }
    val get = ds.connection.getTable(table)
    SelfClosingIterator(resultsToFeatures(get.get(ranges).iterator), get.close)
  }
}

case class MultiRowRangeFilterScanPlan(filter: HBaseFilterStrategyType,
                                       table: TableName,
                                       ranges: Seq[Scan],
                                       remoteFilters: Seq[HBaseFilter] = Nil,
                                       resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {

  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._

    val rowRanges = Lists.newArrayList[RowRange]()
    ranges.foreach { r =>
      rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false))
    }
    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val numRanges = sortedRowRanges.length
    val numThreads = ds.config.queryThreads
    // TODO: parameterize this?
    val rangesPerThread = math.max(1,math.ceil(numRanges/numThreads*2).toInt)
    // TODO: align partitions with region boundaries
    val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

    val groupedScans = groupedRanges.map { localRanges =>
      val mrrf = new MultiRowRangeFilter(localRanges)
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, mrrf)
      remoteFilters.foreach { f => filterList.addFilter(f) }

      val s = new Scan()
      s.setStartRow(localRanges.head.getStartRow)
      s.setStopRow(localRanges.get(localRanges.length-1).getStopRow)
      s.setFilter(filterList)
      s.setCaching(1000)
      s.setCacheBlocks(true)
      s
    }
    // Apply Visibilities
    groupedScans.foreach(ds.applySecurity)
    val results = new HBaseBatchScan(ds.connection, table, groupedScans, ds.config.queryThreads, 100000, Nil)
    SelfClosingIterator(resultsToFeatures(results), results.close)
  }
}