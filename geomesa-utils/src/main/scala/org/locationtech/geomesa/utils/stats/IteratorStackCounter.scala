package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

class IteratorStackCounter extends Stat {
  var count: Long = 1

  override def observe(sf: SimpleFeature): Unit = { }

  override def toJson(): String = ???

  override def add(other: Stat): Stat = {
    other match {
      case o: IteratorStackCounter => count += o.count
    }
    this
  }
}
