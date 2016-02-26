/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.concurrent.CountDownLatch

import com.google.common.util.concurrent.AtomicLongMap
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureStore, SimpleFeatureSource}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class LiveKafkaConsumerFeatureSourceTest extends Specification with HasEmbeddedKafka with LazyLogging {

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  val gf = JTSFactoryFinder.getGeometryFactory

  val zkPath = "/geomesa/kafka/testexpiry"

  val producerParams = Map(
    "brokers"    -> brokerConnect,
    "zookeepers" -> zkConnect,
    "zkPath"     -> zkPath,
    "isProducer" -> true)

  val consumerParams = Map(
    "brokers"    -> brokerConnect,
    "zookeepers" -> zkConnect,
    "zkPath"     -> zkPath,
    "isProducer" -> false)

  "LiveKafkaConsumerFeatureSource" should {
    "allow for configurable expiration" >> {
      val sft = {
        val sft = SimpleFeatureTypes.createType("expiry", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      producerDS.createSchema(sft)

      val expiryConsumer = DataStoreFinder.getDataStore(consumerParams ++ Map(KafkaDataStoreFactoryParams.EXPIRATION_PERIOD.getName -> 1000L))
      val consumerFC = expiryConsumer.getFeatureSource("expiry")

      val fw = producerDS.getFeatureWriter("expiry", null, Transaction.AUTO_COMMIT)
      val sf = fw.next()
      sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
      sf.setDefaultGeometry(gf.createPoint(new Coordinate(0.0, 0.0)))
      fw.write()
      Thread.sleep(500)

      val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

      // verify the feature is written - hit the cache directly
      {
        val features = consumerFC.getFeatures(Filter.INCLUDE).features()
        features.hasNext must beTrue
        val readSF = features.next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")
        features.hasNext must beFalse
      }

      // verify the feature is written - hit the spatial index
      {
        val features = consumerFC.getFeatures(bbox).features()
        features.hasNext must beTrue
        val readSF = features.next()
        sf.getID must be equalTo readSF.getID
        sf.getAttribute("dtg") must be equalTo readSF.getAttribute("dtg")
        features.hasNext must beFalse
      }

      // allow the cache to expire
      Thread.sleep(500)

      // verify feature has expired - hit the cache directly
      {
        val features = consumerFC.getFeatures(Filter.INCLUDE).features()
        features.hasNext must beFalse
      }

      // force the cache cleanup - normally this would happen during additional reads and writes
      consumerFC.asInstanceOf[LiveKafkaConsumerFeatureSource].featureCache.cache.cleanUp()

      // verify feature has expired - hit the spatial index
      {
        val features = consumerFC.getFeatures(bbox).features()
        features.hasNext must beFalse
      }
    }

    "support listeners" >> {
      val m = AtomicLongMap.create[String]()

      val id = "testlistener"
      val numUpdates = 100
      val maxLon = 80.0
      var latestLon = -1.0

      val sft = {
        val sft = SimpleFeatureTypes.createType("listeners", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      producerDS.createSchema(sft)

      val listenerConsumerDS = DataStoreFinder.getDataStore(consumerParams)
      val consumerFC = listenerConsumerDS.getFeatureSource("listeners")

      val latch = new CountDownLatch(numUpdates)

      val featureListener = new TestLambdaFeatureListener((fe: KafkaFeatureEvent) => {
        val f = fe.feature
        val geom: Point = f.getDefaultGeometry.asInstanceOf[Point]
        latestLon = geom.getX
        m.incrementAndGet(f.getID)
        latch.countDown()
      })

      consumerFC.addFeatureListener(featureListener)

      val fw = producerDS.getFeatureWriter("listeners", null, Transaction.AUTO_COMMIT)

      (numUpdates to 1 by -1).foreach { writeUpdate }

      def writeUpdate(i: Int) = {
        val ll = maxLon - maxLon/i

        val sf = fw.next()
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID("testlistener")
        sf.setAttributes(Array("smith", 30, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
        sf.setDefaultGeometry(gf.createPoint(new Coordinate(ll, ll)))
        fw.write()
      }

      logger.debug("Wrote feature")
//      while(latch.getCount > 0) {
        Thread.sleep(100)
//      }

      logger.debug("getting id")
      m.get(id) must be equalTo numUpdates
      latestLon must be equalTo 0.0
    }

    "handle multiple consumers starting in-between CRUD messages" >> {
      val ff = CommonFactoryFinder.getFilterFactory2
      val sftName = "concurrent"
      val wait    = 500

      val sft = {
        val sft = SimpleFeatureTypes.createType(sftName, "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)
      }
      val producerDS = DataStoreFinder.getDataStore(producerParams)
      producerDS.createSchema(sft)
      val producerFS = producerDS.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]

      val fw = producerDS.getFeatureWriter(sftName, null, Transaction.AUTO_COMMIT)

      def writeUpdate(x: Double, y: Double, name: String, age: Int, id: String) = {
        Thread.sleep(wait)
        val sf = fw.next()
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
        sf.setAttributes(Array(name, age, DateTime.now().toDate).asInstanceOf[Array[AnyRef]])
        sf.setDefaultGeometry(gf.createPoint(new Coordinate(x, y)))
        fw.write()
        Thread.sleep(wait)
      }

      def deleteById(id: String) = {
        Thread.sleep(wait)
        producerFS.removeFeatures(ff.id(ff.featureId(id)))
        Thread.sleep(wait)
      }

      val listenerConsumerDS = DataStoreFinder.getDataStore(consumerParams)
      val consumers = mutable.ListBuffer[SimpleFeatureSource]()

      def addConsumer()  {
        consumers += listenerConsumerDS.getFeatureSource(sftName)
        logger.info("Got a new Consumer")
      }

      def checkConsumers[T](check: (SimpleFeatureSource) => T) = {
        Thread.sleep(wait)
        println(s"Size of consumers = ${consumers.size}")
        consumers.map { check }
      }

      addConsumer
      writeUpdate(0.0, 0.0, "James", 33, "1")
      checkConsumers {_.getFeatures().features().toList.size must equalTo(1) }

      addConsumer
      deleteById("1")
      checkConsumers {_.getFeatures().features().toList.size must equalTo(0) }

      addConsumer
      writeUpdate(1.0, -1.0, "Mark", 27, "2")
      checkConsumers {_.getFeatures().features().toList.size must equalTo(1) }


    }
  }

  step {
    shutdown()
  }
}
