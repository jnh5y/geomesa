package org.locationtech.geomesa.trajectory

import org.geotools.data.Query

class TrajectoryStore {
  val buf = scala.collection.mutable.ListBuffer.empty[Trajectory]

  def add(traj: Trajectory) = {
    buf.append(traj)
  }

  def getTrajectories(query: Query): Seq[Trajectory] = {
    val filter = query.getFilter
    buf.filter(t => filter.evaluate(t.sf))
  }
}
