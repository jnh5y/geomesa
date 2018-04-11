package org.locationtech.geomesa.trajectory

import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case class Trajectory(obs: Array[SimpleFeature]) {
  val sft: SimpleFeatureType = obs.head.getFeatureType
  val sf: SimpleFeature = ???

  // TODO: Implement configurable/variable ways of specifying the dtg/geom attributes
  // TODO: Implement getAttribute by index rather than
  val dtgAttribute = "dtg"
  val geomAttribute = "geom"

  def length: Int = obs.length
  def getObs(n: Int): SimpleFeature = obs(n)

  def getStart: SimpleFeature = obs(0)
  def getEnd: SimpleFeature = obs(obs.length-1)

  def getStartTime: Date = getStart.getAttribute(dtgAttribute).asInstanceOf[Date]
  def getEndTime: Date   = getEnd.getAttribute(dtgAttribute).asInstanceOf[Date]

  def getStartPoint: Point = getStart.getAttribute(geomAttribute).asInstanceOf[Point]
  def getEndPoint: Point   = getEnd.getAttribute(geomAttribute).asInstanceOf[Point]
}

object Trajectory {
  val TrajectoryTypeName  = "trajectory"
  val EntityIdFieldName   = "entityId"
  val StartDateFieldName  = "startDate"
  val EndDateFieldName    = "endDate"
  val PathFieldName       = "path"
  val StartPointFieldName = "startPoint"
  val EndPointFieldName   = "endPoint"
  
  val trajSFT = new SftBuilder()
    .stringType(EntityIdFieldName)
    .date(StartDateFieldName, true)
    .date(EndDateFieldName)
    .geometry(PathFieldName, true)
    .geometry(StartPointFieldName)
    .geometry(EndPointFieldName)
    .build(TrajectoryTypeName)


  // Constructor to sort dtg
  // TODO: Implement this and sensible companions which allow for configuration of
  // DTG field used for sorting
  //def apply(unsortedObs: Seq[SimpleFeature]) = Trajectory(unsortedObs.sortBy())
}
