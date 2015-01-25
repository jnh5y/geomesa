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

import java.awt.image.{BufferedImage, Raster => JRaster, RenderedImage, WritableRaster}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.{Hashtable => JHashtable}
import javax.media.jai.remote.SerializableRenderedImage

import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory, GridGeometry2D}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.imgscalr.Scalr._
import org.joda.time.DateTime
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.{Raster, RasterQuery, RasterStore}
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.opengis.geometry.Envelope

import scala.reflect.runtime.universe._

object RasterUtils {

  //TODO: WCS: Split off functions useful for just tests into a separate object, which includes classes from here on down
  val white     = Array[Int] (255, 255, 255)
  val lightGray = Array[Int] (200, 200, 200)
  val gray      = Array[Int] (128, 128, 128)
  val darkGray  = Array[Int] (54, 54, 54)
  val black     = Array[Int] (0, 0, 0)

  object IngestRasterParams {
    val ACCUMULO_INSTANCE   = "geomesa-tools.ingestraster.instance"
    val ZOOKEEPERS          = "geomesa-tools.ingestraster.zookeepers"
    val ACCUMULO_MOCK       = "geomesa-tools.ingestraster.useMock"
    val ACCUMULO_USER       = "geomesa-tools.ingestraster.user"
    val ACCUMULO_PASSWORD   = "geomesa-tools.ingestraster.password"
    val AUTHORIZATIONS      = "geomesa-tools.ingestraster.authorizations"
    val VISIBILITIES        = "geomesa-tools.ingestraster.visibilities"
    val FILE_PATH           = "geomesa-tools.ingestraster.path"
    val FORMAT              = "geomesa-tools.ingestraster.format"
    val TIME                = "geomesa-tools.ingestraster.time"
    val GEOSERVER_REG       = "geomesa-tools.ingestraster.geoserver.reg"
    val TABLE               = "geomesa-tools.ingestraster.table"
    val PARLEVEL            = "geomesa-tools.ingestraster.parallel.level"
  }

  def imageSerialize(image: RenderedImage): Array[Byte] = {
    val buffer: ByteArrayOutputStream = new ByteArrayOutputStream
    val out: ObjectOutputStream = new ObjectOutputStream(buffer)
    val serializableImage = new SerializableRenderedImage(image, true)
    try {
      out.writeObject(serializableImage)
    } finally {
      out.close
    }
    buffer.toByteArray
  }

  def imageDeserialize(imageBytes: Array[Byte]): RenderedImage = {
    val in: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(imageBytes))
    var read: RenderedImage = null
    try {
      read = in.readObject.asInstanceOf[RenderedImage]
    } finally {
      in.close
    }
    read
  }

  val defaultGridCoverageFactory = new GridCoverageFactory

  def renderedImageToGridCoverage2d(name: String, image: RenderedImage, env: Envelope): GridCoverage2D =
    defaultGridCoverageFactory.create(name, image, env)

  def allocateBufferedImage(width: Int, height: Int, chunk: RenderedImage): BufferedImage = {
    val properties = new JHashtable[String, Object]
    if (chunk.getPropertyNames != null) {
      chunk.getPropertyNames.foreach(name => properties.put(name, chunk.getProperty(name)))
    }
    val colorModel = chunk.getColorModel
    val alphaPremultiplied = colorModel.isAlphaPremultiplied
    val sampleModel = chunk.getSampleModel.createCompatibleSampleModel(width, height)
    val emptyRaster = JRaster.createWritableRaster(sampleModel, null)
    new BufferedImage(colorModel, emptyRaster, alphaPremultiplied, properties)
  }

  def getEmptyImage(width: Int, height: Int): BufferedImage = {
    new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
  }

  def populateMosaic(mosaic: BufferedImage, raster: Raster, env: Envelope, resX: Double, resY: Double) = {
    val rasterEnv = raster.referencedEnvelope.intersection(envelopeToReferencedEnvelope(env))
    val mosaicXres = env.getSpan(0) / mosaic.getWidth
    val mosaicYres = env.getSpan(1) / mosaic.getHeight
    val cropped = cropRaster(raster, env)
    val scaled = rescaleBufferedImage(mosaicXres, mosaicYres, cropped)
    val originX = Math.ceil((rasterEnv.getMinX - env.getMinimum(0)) / mosaicXres).toInt
    val originY = Math.ceil((env.getMaximum(1) - rasterEnv.getMaxY) / mosaicYres).toInt
    mosaic.getRaster.setRect(originX, originY, scaled.getData)
    scaled.flush()
  }

  def evenBetterMosaic(chunks: Iterator[Raster], queryWidth: Int, queryHeight: Int,
                       resX: Double, resY: Double, queryEnv: Envelope): BufferedImage = {
    if (chunks.isEmpty) {
      getEmptyImage(queryWidth, queryHeight)
    } else {
      val rescaleX = resX / (queryEnv.getSpan(0) / queryWidth)
      val rescaleY = resY / (queryEnv.getSpan(1) / queryHeight)
      val imageWidth = Math.max(Math.round(queryWidth / rescaleX), 1).toInt
      val imageHeight = Math.max(Math.round(queryHeight / rescaleY), 1).toInt
      val firstRaster = chunks.next()
      val mosaic = allocateBufferedImage(imageWidth, imageHeight, firstRaster.chunk)
      populateMosaic(mosaic, firstRaster, queryEnv, resX, resY)
      while (chunks.hasNext) {
        populateMosaic(mosaic, chunks.next(), queryEnv, resX, resY)
      }
      rescaleBufferedImage(rescaleX, rescaleY, mosaic)
    }
  }

  def rescaleBufferedImage(rescaleX: Double, rescaleY: Double, image: BufferedImage): BufferedImage = {
    val xScaled = (image.getWidth * rescaleX).toInt
    val yScaled = (image.getHeight * rescaleY).toInt
    val result = resize(image, Method.SPEED, xScaled, yScaled, null)
    image.flush()
    result
  }

  def cropRaster(raster: Raster, cropEnv: Envelope): BufferedImage = {
    // need to check if intersection is valid (ie, not just two corners touching
    val rasterEnv = raster.referencedEnvelope
    val intersection = rasterEnv.intersection(envelopeToReferencedEnvelope(cropEnv))
    val chunkXRes = rasterEnv.getWidth / raster.chunk.getWidth
    val chunkYRes = rasterEnv.getHeight / raster.chunk.getHeight
    val w = (intersection.getWidth / chunkXRes).toInt
    val h = (intersection.getHeight / chunkYRes).toInt
    val uLX = Math.ceil((intersection.getMinX - rasterEnv.getMinimum(0)) / chunkXRes).toInt
    val uLY = Math.ceil((rasterEnv.getMaximum(1) - intersection.getMaxY) / chunkYRes).toInt
    val b = asBufferedImage(raster.chunk)
    val result = crop(b, uLX, uLY, w, h, null)
    b.flush()
    result
  }

  def asBufferedImage(r: RenderedImage): BufferedImage = {
    val bufferedResult = allocateBufferedImage(r.getWidth, r.getHeight, r)
    bufferedResult.getRaster.setRect(0, 0, r.getData)
    bufferedResult
  }

  def envelopeToReferencedEnvelope(e: Envelope): ReferencedEnvelope = {
    new ReferencedEnvelope(e.getMinimum(0), e.getMaximum(0), e.getMinimum(1), e.getMaximum(1), CRS.decode("EPSG:4326"))
  }

  def mosaicRasters(rasters: Iterator[Raster], width: Int, height: Int,
                    env: Envelope, resX: Double, resY: Double): BufferedImage = {
    if (rasters.isEmpty) {
      getEmptyImage(width, height)
    } else {
      val rescaleX = resX / (env.getSpan(0) / width)
      val rescaleY = resY / (env.getSpan(1) / height)
      val scaledWidth = width / rescaleX
      val scaledHeight = height / rescaleY
      val imageWidth = Math.max(Math.round(scaledWidth), 1).toInt
      val imageHeight = Math.max(Math.round(scaledHeight), 1).toInt
      val firstRaster = rasters.next()
      val mosaic = allocateBufferedImage(imageWidth, imageHeight, firstRaster.chunk)
      populateMosaic(mosaic, firstRaster, env, resX, resY)
      while (rasters.hasNext) {
        val raster = rasters.next()
        populateMosaic(mosaic, raster, env, resX, resY)
      }
      mosaic
    }
  }

  def getNewImage[T: TypeTag](w: Int, h: Int, fill: Array[T],
                              imageType: Int = BufferedImage.TYPE_BYTE_GRAY): BufferedImage = {
    val image = new BufferedImage(w, h, imageType)
    val wr = image.getRaster
    val setPixel: (Int, Int) => Unit = typeOf[T] match {
      case t if t =:= typeOf[Int]    =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Int]])
      case t if t =:= typeOf[Float]  =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Float]])
      case t if t =:= typeOf[Double] =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Double]])
      case _                         =>
        throw new IllegalArgumentException(s"Error, cannot handle Arrays of type: ${typeOf[T]}")
    }

    for (i <- 0 until h; j <- 0 until w) { setPixel(i, j) }
    image
  }

  def imageToCoverage(img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  def createRasterStore(tableName: String) = {
    val rs = RasterStore("user", "pass", "testInstance", "zk", tableName, "SUSA", "SUSA", true)
    rs
  }

  def generateQuery(minX: Double, maxX: Double, minY: Double, maxY: Double, res: Double = 10.0) = {
    val bb = BoundingBox(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, res, None, None)
  }

  def generateTestRaster(minX: Double, maxX: Double, minY: Double, maxY: Double,
                         w: Int = 256, h: Int = 256, res: Double = 10.0,
                         color: Array[Int] = white): Raster = {
    val ingestTime = new DateTime()
    val env = new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84)
    val bbox = BoundingBox(env)
    val metadata = DecodedIndex(Raster.getRasterId("testRaster"), bbox.geom, Option(ingestTime.getMillis))
    val image = getNewImage(w, h, color)
    val coverage = imageToCoverage(image.getRaster, env, defaultGridCoverageFactory)
    new Raster(coverage.getRenderedImage, metadata, res)
  }

  def generateTestRasterFromBoundingBox(bbox: BoundingBox, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    generateTestRaster(bbox.minLon, bbox.maxLon, bbox.minLat, bbox.maxLat, w, h, res)
  }

  def generateTestRasterFromGeoHash(gh: GeoHash, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    generateTestRasterFromBoundingBox(gh.bbox, w, h, res)
  }

  case class sharedRasterParams(gg: GridGeometry2D, envelope: Envelope) {
    val width = gg.getGridRange2D.getWidth
    val height = gg.getGridRange2D.getHeight
    val resX = (envelope.getMaximum(0) - envelope.getMinimum(0)) / width
    val resY = (envelope.getMaximum(1) - envelope.getMinimum(1)) / height
    val suggestedQueryResolution = math.min(resX, resY)
  }
}

