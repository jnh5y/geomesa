package org.locationtech.geomesa.raster.data


import org.junit.runner.RunWith
import org.locationtech.geomesa.raster._
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeohashUtils, GeoHash}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloRasterQueryPlannerTest extends Specification {

  sequential

  val schema = RasterIndexSchema("")
  val availableResolutions = List[Double](45.0/256.0, 45.0/1024.0)

  val arqp = AccumuloRasterQueryPlanner(schema, availableResolutions)

  val testCases = List(
    (128, 45.0/256.0),
    (156, 45.0/256.0),
    (201, 45.0/256.0),
    (256, 45.0/256.0),
    (257, 45.0/1024.0),
    (432, 45.0/1024.0),
    (512, 45.0/1024.0),
    (1000, 45.0/1024.0),
    (1024, 45.0/1024.0),
    (1025, 45.0/1024.0),
    (2000, 45.0/1024.0)
  )

  // Query planner should return a valid resolution.
  "RasterQueryPlanner" should {
    "round down" in {

      testCases.map {
        case (size, expected) =>
          runTest(size, expected)
      }
    }
  }

  // Query planner should round up/down???

  def runTest(size: Int, expectedResolution: Double): MatchResult[Double] = {
    val q1 = RasterUtils.generateQuery(0, 45, 0, 45, 45.0/size)
    val qp = arqp.getQueryPlan(q1)

    val rangeString = qp.ranges.head.getStartKey.getRow.toString
    val encodedDouble = rangeString.split("~")(1)

    val queryResolution = lexiDecodeStringToDouble(encodedDouble)

    println(s"Query pixel size: $size. Expected query resolution: $expectedResolution.  " +
      s"Returned query resolution $queryResolution.")

    val roundedResolution = BigDecimal(expectedResolution).setScale(7, BigDecimal.RoundingMode.HALF_UP).toDouble

    queryResolution should be equalTo roundedResolution
  }



}
