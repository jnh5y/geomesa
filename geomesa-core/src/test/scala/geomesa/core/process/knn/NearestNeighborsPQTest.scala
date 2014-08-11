package geomesa.core.process.knn


import geomesa.core._
import geomesa.core.index.Constants
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class NearestNeighborsPQTest extends Specification {

  val sftName = "geomesaKNNTestQueryFeature"
  val sft = SimpleFeatureTypes.createType(sftName, index.spec)

  val equatorSF = SimpleFeatureBuilder.build(sft, List(), "equator")
  equatorSF.setDefaultGeometry(WKTUtils.read(f"POINT(0.1 0.2)"))
  equatorSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val midpointSF = SimpleFeatureBuilder.build(sft, List(), "midpoint")
  midpointSF.setDefaultGeometry(WKTUtils.read(f"POINT(45.1 45.1)"))
  midpointSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val polarSF = SimpleFeatureBuilder.build(sft, List(), "polar")
  polarSF.setDefaultGeometry(WKTUtils.read(f"POINT(89.9 89.9)"))
  polarSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  val polarSF2 = SimpleFeatureBuilder.build(sft, List(), "polar2")
  polarSF2.setDefaultGeometry(WKTUtils.read(f"POINT(0.0001 89.9)"))
  polarSF2.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  def diagonalFeatureCollection: DefaultFeatureCollection = {
    val sftName = "geomesaKNNTestDiagonalFeature"
    val sft = SimpleFeatureTypes.createType(sftName, index.spec)
    sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = DEFAULT_DTG_PROPERTY_NAME

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    val constantDate = new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate
    // generate a range of Simple Features along a "diagonal"
    Range(0, 91).foreach { lat =>
      val sf = SimpleFeatureBuilder.build(sft, List(), lat.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute(DEFAULT_DTG_PROPERTY_NAME, constantDate)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
    featureCollection
  }

  def polarFeatureCollection: DefaultFeatureCollection = {
    val sftName = "geomesaKNNTestPolarFeature"
    val sft = SimpleFeatureTypes.createType(sftName, index.spec)
    sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = DEFAULT_DTG_PROPERTY_NAME

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    val constantDate = new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate
    val polarLat = 89.9
    // generate a range of Simple Features along a "diagonal"
    Range(-180, 180).foreach { lon =>
      val sf = SimpleFeatureBuilder.build(sft, List(), lon.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lon%d $polarLat)"))
      sf.setAttribute(DEFAULT_DTG_PROPERTY_NAME, constantDate)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
    featureCollection
  }


  "Geomesa NearestNeighbor PriorityQueue" should {
    "find things close by the equator" in {
      import geomesa.utils.geotools.Conversions._
      val equatorPQ = NearestNeighbors(equatorSF, 10)
      equatorPQ ++= diagonalFeatureCollection.features.map {
        sf=> SimpleFeatureWithDistance(sf,equatorPQ.distance(sf))
      }
      equatorPQ.head.sf.getID must equalTo("0")
    }

    "find things close by Southwest Russia" in {
      import geomesa.utils.geotools.Conversions._
      val midpointPQ = NearestNeighbors(midpointSF, 10)
      midpointPQ ++= diagonalFeatureCollection.features.map {
        sf=> SimpleFeatureWithDistance(sf,midpointPQ.distance(sf))
      }

      midpointPQ.head.sf.getID must equalTo("45")
    }

    "find things close by the North Pole" in {
      import geomesa.utils.geotools.Conversions._
      val polarPQ = NearestNeighbors(polarSF, 10)
      polarPQ ++= diagonalFeatureCollection.features.map{
        sf=> SimpleFeatureWithDistance(sf,polarPQ.distance(sf))
      }

      polarPQ.head.sf.getID must equalTo("90")
    }

    "find things in the north polar region" in {
      import geomesa.utils.geotools.Conversions._
      val polarPQ = NearestNeighbors(polarSF, 10)
      polarPQ ++= polarFeatureCollection.features.map {
        sf=> SimpleFeatureWithDistance(sf,polarPQ.distance(sf))
      }

      polarPQ.head.sf.getID must equalTo("90")
    }

    "find more things near the north polar region" in {
      import geomesa.utils.geotools.Conversions._
      val polarPQ = NearestNeighbors(polarSF2, 10)
      polarPQ ++= polarFeatureCollection.features.map {
        sf=> SimpleFeatureWithDistance(sf,polarPQ.distance(sf))
      }

      polarPQ.head.sf.getID must equalTo("0")
    }
  }
}