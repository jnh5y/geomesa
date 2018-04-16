/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.trajectory

import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.AttributeExpressionImpl
import org.locationtech.geomesa.trajectory.Trajectory._
import org.locationtech.geomesa.trajectory.TrajectoryStore._
import org.opengis.geometry.BoundingBox

class TrajectoryStore {
  val buf = scala.collection.mutable.ListBuffer.empty[Trajectory]

  def add(traj: Trajectory) = {
    buf.append(traj)
  }

  def getTrajectories(query: Query): Seq[Trajectory] = {
    val filter = query.getFilter
    buf.filter(t => filter.evaluate(t.sf))
  }

  def getTrajectoriesIntersectingBBOX(bbox: BoundingBox): Seq[Trajectory] = {
    val q = new Query()
    q.setFilter(ff.bbox(new AttributeExpressionImpl(PathFieldName), bbox))
    getTrajectories(q)
  }
}

object TrajectoryStore {
  val ff = CommonFactoryFinder.getFilterFactory2()
}
