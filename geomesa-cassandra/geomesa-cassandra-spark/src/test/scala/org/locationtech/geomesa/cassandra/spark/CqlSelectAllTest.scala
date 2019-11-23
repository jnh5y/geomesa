/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.spark

import java.nio.file.{Files, Path}
import java.util

import com.datastax.driver.core.{Cluster, SocketOptions}
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.spark.{SparkConf, SparkContext}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.apache.hadoop.mapreduce._
import org.locationtech.geomesa.cassandra.jobs.CassandraJobUtils
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import com.datastax.driver.core.Row
import com.typesafe.scalalogging.LazyLogging
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.locationtech.geomesa.jobs.GeoMesaConfigurator

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CqlSelectAllTest extends Specification {

  sequential

  var storage: Path = _
  var params: Map[String, String] = _
  var ds: CassandraDataStore = _

  var sc: SparkContext = _

  step {
    // cassandra database set up
    storage = Files.createTempDirectory("cassandra")

    System.setProperty("cassandra.storagedir", storage.toString)

    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 1200000L)

    var readTimeout: Int = SystemProperty("cassandraReadTimeout", "12000").get.toInt

    if (readTimeout < 0) {
      readTimeout = 12000
    }
    val host = EmbeddedCassandraServerHelper.getHost
    val port = EmbeddedCassandraServerHelper.getNativeTransportPort
    val cluster = new Cluster.Builder().addContactPoints(host).withPort(port)
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout)).build().init()
    val session = cluster.connect()
    val cqlDataLoader = new CQLDataLoader(session)
    cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))

    // add geotools flag to param map for Spark
    params = Map(
      Params.ContactPointParam.getName -> s"$host:$port",
      Params.KeySpaceParam.getName -> "geomesa_cassandra",
      Params.CatalogParam.getName -> "test_sft"
    )
    ds = DataStoreFinder.getDataStore(params).asInstanceOf[CassandraDataStore]

    // Spark set up
    val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    sc = SparkContext.getOrCreate(conf)
  }

  lazy val chicagoSft =
    SimpleFeatureTypes.createType("chicago",
      "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  lazy val chicagoFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
    ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
    ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", "POINT (-73.5 39.5)"),
    ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", "POINT (-74.0 35.5)"),
    ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", "POINT (-79.0 37.5)")
  )

  "The CassandraSpatialRDDProvider" should {
    "read from the embedded Cassandra database" in {
      ds.createSchema(chicagoSft)
      WithClose(ds.getFeatureWriterAppend("chicago", Transaction.AUTO_COMMIT)) { writer =>
        chicagoFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val job: Job = Job.getInstance(new Configuration(),"selectall")  // instantiate new job

      ConfigHelper.setInputInitialAddress(job.getConfiguration, "localhost")

      ConfigHelper.setInputColumnFamily(job.getConfiguration, "geomesa_cassandra", "chicago")
      job.setInputFormatClass(classOf[CqlInputFormat])
      CqlConfigHelper.setInputCql(job.getConfiguration, "select * from " + "chicago")

      job.setMapOutputKeyClass(classOf[Text])
      job.setMapOutputValueClass(classOf[SimpleFeature])
      ConfigHelper.setInputPartitioner(job.getConfiguration, "Murmur3Partitioner")

//      GeoMesaConfigurator.setResultsToFeatures(conf, qp.resultsToFeatures)

      //      val OUTPUT_PLAN_PREFIX = "tmp/read_results"
//      FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX))
//      job.waitForCompletion(true)

      val rdd = new NewHadoopRDD(sc, classOf[GeoMesaCassandraInputFormat], classOf[Text], classOf[SimpleFeature], job.getConfiguration).map(_._2)
      rdd.count
      true mustEqual(true)
    }

  }

}