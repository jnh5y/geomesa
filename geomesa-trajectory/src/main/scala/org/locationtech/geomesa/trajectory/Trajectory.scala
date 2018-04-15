/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.trajectory

import java.util.Date

import com.vividsolutions.jts.geom.{GeometryFactory, LineString, Point}
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features._

case class Trajectory(obs: Array[SimpleFeature]) {
  lazy val sft: SimpleFeatureType = obs.head.getFeatureType
  lazy val sf: SimpleFeature = ScalaSimpleFeature.create(Trajectory.trajSFT,
    "ID",
    "EntityID",
    getStartDate(),
    getEndDate(),
    getPath(),
    getStartPoint(),
    getEndPoint())

  // TODO: Implement configurable/variable ways of specifying the dtg/geom attributes
  // TODO: Implement getAttribute by index rather than
  val dtgAttribute = "dtg"
  val geomAttribute = "geom"

  def length: Int = obs.length
  def getObs(n: Int): SimpleFeature = obs(n)

  def getStart(): SimpleFeature = obs(0)
  def getEnd(): SimpleFeature = obs(obs.length-1)

  def getStartDate(): Date = getStart.getAttribute(dtgAttribute).asInstanceOf[Date]
  def getEndDate(): Date   = getEnd.getAttribute(dtgAttribute).asInstanceOf[Date]

  def getStartPoint(): Point = getStart.getAttribute(geomAttribute).asInstanceOf[Point]
  def getEndPoint(): Point   = getEnd.getAttribute(geomAttribute).asInstanceOf[Point]
  def getPath(): LineString  = {
    val coords = obs.map(_.getAttribute(geomAttribute).asInstanceOf[Point].getCoordinate)
    println(s"Coords: ${coords.length}")
    Trajectory.gf.createLineString(coords)
  }
}

object Trajectory {
  val gf = new GeometryFactory()

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
