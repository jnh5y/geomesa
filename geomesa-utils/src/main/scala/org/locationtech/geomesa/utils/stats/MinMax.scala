package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature


class MinMax[T <: Comparable[T]](val attribute: String) extends Stat {

  var min: T = _
  var max: T = _

  override def observe(sf: SimpleFeature): Unit = {

    val sfval = sf.getAttribute(attribute)

    if (sfval != null) {
      updateMin(sfval.asInstanceOf[T])
      updateMax(sfval.asInstanceOf[T])
    }
  }

  override def add(other: Stat): Stat = {

    other match {
      case mm: MinMax[T] =>
        updateMin(mm.min)
        updateMax(mm.max)
    }

    this
  }

  private def updateMin(sfval : T): Unit = {
    if (min == null) {
      min = sfval
    } else {
      if (min.compareTo(sfval) > 0) {
        min = sfval
      }
    }
  }

  private def updateMax(sfval : T): Unit = {
    if (max == null) {
      max = sfval
    } else {
      if (max.compareTo(sfval) < 0) {
        max = sfval
      }
    }
  }


  override def pack(): Array[Byte] = ???

  override def toJson(): String = s"$attribute: { min: $min, max: $max }"
}