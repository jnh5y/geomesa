package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

class RangeHistogram extends Stat {
  override def observe(sf: SimpleFeature): Unit = ???

  override def toJson(): String = ???

  override def add(other: Stat): Stat = ???
}
