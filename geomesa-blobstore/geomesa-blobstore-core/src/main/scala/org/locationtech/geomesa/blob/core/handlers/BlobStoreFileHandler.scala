package org.locationtech.geomesa.blob.core.handlers

import java.io.File
import java.util
import java.util.Date
import javax.imageio.spi.ServiceRegistry

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.accumulo.util.{Z3FeatureIdGenerator, Z3UuidGenerator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object BlobStoreFileHandler {
  def buildSF(file: File, params: Map[String, String]): Option[SimpleFeature] = {
    val handlers = ServiceRegistry.lookupProviders(classOf[FileHandler])

    handlers.find(_.canProcess(file, params)).map(_.buildSF(file, params))
  }
}

trait AbstractFileHandler extends FileHandler {
  val builder = new SimpleFeatureBuilder(sft)
  val featureIdGenerator = new Z3FeatureIdGenerator

  override def buildSF(file: File, params: util.Map[String, String]): SimpleFeature = {
    val wkt = getGeometry(file, params)
    val dtg = getDate(file, params)
    val z3id = Z3UuidGenerator.createUuid(wkt, dtg.getTime)

    builder.set(filenameFieldName, file.getName)
    builder.set(geomeFieldName, wkt)
    builder.set(idFieldName, z3id)
    builder.set(dateFieldName, dtg)

    builder.buildFeature("")
  }

  def getDate(file: File, params: util.Map[String, String]): Date = {
    getDateFromFile(file).orElse(getDateFromParams(params)).getOrElse(new Date(System.currentTimeMillis()))
  }

  def getDateFromFile(file: File): Option[Date] = None
  def getDateFromParams(params: util.Map[String, String]): Option[Date] = None

  def getGeometry(file: File, params: util.Map[String, String]): Geometry = {
    getGeometryFromFile(file).orElse(getGeometryFromParams(params)).getOrElse{
      throw new Exception(s"Could not get Geometry for $file with params $params.")
    }
  }

  def getGeometryFromFile(file: File): Option[Geometry] = None
  def getGeometryFromParams(params: util.Map[String, String]): Option[Geometry] = None
}