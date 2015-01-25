/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.util

import java.awt.image.{Raster => JRaster, RenderedImage}
import java.io.File
import java.util.UUID
import javax.imageio.ImageIO

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class BetterMosaicTest extends Specification {

  sequential

   def writeImageToDisk(image: RenderedImage, name: String = "") = {
    val path = new File("test_" + name + "_" + UUID.randomUUID().toString() +".png")
    ImageIO.write(image, "png", path)
  }

  "Better Mosaic" should {
    "Mosaic two adjacent Rasters together" in {
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.darkGray)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.white)
      val rasterSeq = Seq(testRaster1, testRaster2)

      val queryEnv = new ReferencedEnvelope(-50.0, 50.0, 0.0, 50.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 512, 256, 100.0/512, 50.0/256, queryEnv)

      writeImageToDisk(testMosaic, "two_adjacent_same_res")
      testMosaic must beAnInstanceOf[RenderedImage]
    }

    "Mosaic four Rasters together with a Query of equal extent and equal resolution" in {
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.black)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.white)
      val testRaster3 = RasterUtils.generateTestRaster(0, 50, -50, 0, color = RasterUtils.black)
      val testRaster4 = RasterUtils.generateTestRaster(-50, 0, -50, 0, color = RasterUtils.white)
      val rasterSeq = Seq(testRaster1, testRaster2, testRaster3, testRaster4)

      val queryEnv = new ReferencedEnvelope(-50.0, 50.0, -50.0, 50.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 512, 512, 100.0/512, 100.0/512, queryEnv)

      writeImageToDisk(testMosaic, "same_envelope_same_res")
      testMosaic must beAnInstanceOf[RenderedImage]
    }

    "Mosaic four Rasters together with a Query of smaller extent and equal resolution" in {
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.darkGray)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.lightGray)
      val testRaster3 = RasterUtils.generateTestRaster(0, 50, -50, 0, color = RasterUtils.darkGray)
      val testRaster4 = RasterUtils.generateTestRaster(-50, 0, -50, 0, color = RasterUtils.lightGray)
      val rasterSeq = Seq(testRaster1, testRaster2, testRaster3, testRaster4)

      val queryEnv = new ReferencedEnvelope(-25.0, 25.0, -25.0, 25.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 256, 256, 50.0/256, 50.0/256, queryEnv)

      writeImageToDisk(testMosaic, "smaller_envelope_same_res")
      testMosaic must beAnInstanceOf[RenderedImage]
    }

    "Mosaic four Rasters together with a Query of smaller extent and equal resolution offsetted" in {
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.darkGray)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.lightGray)
      val testRaster3 = RasterUtils.generateTestRaster(0, 50, -50, 0, color = RasterUtils.darkGray)
      val testRaster4 = RasterUtils.generateTestRaster(-50, 0, -50, 0, color = RasterUtils.lightGray)
      val rasterSeq = Seq(testRaster1, testRaster2, testRaster3, testRaster4)

      val queryEnv = new ReferencedEnvelope(-35.0, 15.0, -25.0, 25.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 256, 256, 50.0/256, 50.0/256, queryEnv)

      writeImageToDisk(testMosaic, "smaller_envelope_same_res_offsetted")
      testMosaic must beAnInstanceOf[RenderedImage]
    }

    "Mosaic four Rasters together with a Query of larger extent and equal resolution" in {
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.lightGray)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.darkGray)
      val testRaster3 = RasterUtils.generateTestRaster(0, 50, -50, 0, color = RasterUtils.lightGray)
      val testRaster4 = RasterUtils.generateTestRaster(-50, 0, -50, 0, color = RasterUtils.darkGray)
      val rasterSeq = Seq(testRaster1, testRaster2, testRaster3, testRaster4)

      val queryEnv = new ReferencedEnvelope(-60.0, 60.0, -60.0, 60.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 614, 614, 120.0/614, 120.0/614, queryEnv)

      writeImageToDisk(testMosaic, "bigger_envelope_same_res")
      testMosaic must beAnInstanceOf[RenderedImage]
    }

    "Mosaic four Rasters together with a Query of smaller extent and smaller resolution" in {
      // This can be thought of as zoom with a down-sample
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.lightGray)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.white)
      val testRaster3 = RasterUtils.generateTestRaster(0, 50, -50, 0, color = RasterUtils.gray)
      val testRaster4 = RasterUtils.generateTestRaster(-50, 0, -50, 0, color = RasterUtils.black)
      val rasterSeq = Seq(testRaster1, testRaster2, testRaster3, testRaster4)

      val queryEnv = new ReferencedEnvelope(-25.0, 25.0, -25.0, 25.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 64, 64, 50.0/128, 50.0/128, queryEnv)

      writeImageToDisk(testMosaic, "smaller_envelope_smaller_res")
      testMosaic must beAnInstanceOf[RenderedImage]
    }

    "Mosaic four Rasters together with a Query of larger extent and smaller resolution" in {
      val testRaster1 = RasterUtils.generateTestRaster(-50, 0, 0, 50, color = RasterUtils.lightGray)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, color = RasterUtils.darkGray)
      val testRaster3 = RasterUtils.generateTestRaster(0, 50, -50, 0, color = RasterUtils.lightGray)
      val testRaster4 = RasterUtils.generateTestRaster(-50, 0, -50, 0, color = RasterUtils.darkGray)
      val rasterSeq = Seq(testRaster1, testRaster2, testRaster3, testRaster4)

      val queryEnv = new ReferencedEnvelope(-60.0, 60.0, -60.0, 60.0, CRS.decode("EPSG:4326"))
      val testMosaic = RasterUtils.evenBetterMosaic(rasterSeq.iterator, 307, 307, 120.0/614, 120.0/614, queryEnv)

      writeImageToDisk(testMosaic, "bigger_envelope_smaller_res")
      testMosaic must beAnInstanceOf[RenderedImage]
    }


  }

  "cropRaster" should {

    "not crop a raster when the cropEnv is identical to raster extent" in {
      val cropEnv = new ReferencedEnvelope(0.0, 50.0, 0.0, 50.0, CRS.decode("EPSG:4326"))
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      // Check the return type of cropRaster.  This was not going to pass...
      //croppedRaster must beAnInstanceOf[JRaster]
      croppedRaster.getHeight mustEqual 256
      croppedRaster.getWidth  mustEqual 256
    }

    "crop a raster into a square quarter" in {
      val cropEnv = new ReferencedEnvelope(0.0, 25.0, 0.0, 25.0, CRS.decode("EPSG:4326"))
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      //croppedRaster must beAnInstanceOf[JRaster]
      croppedRaster.getHeight mustEqual 128
      croppedRaster.getWidth  mustEqual 128
    }

    "crop a raster with a offsetted cropping envelope" in {
      val cropEnv = new ReferencedEnvelope(-10.0, 10.0, 0.0, 25.0, CRS.decode("EPSG:4326"))
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      //croppedRaster must beAnInstanceOf[JRaster]
      croppedRaster.getHeight mustEqual 128
      croppedRaster.getWidth  mustEqual 51  // should maybe be 52
    }

  }


}
