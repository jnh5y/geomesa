package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

class IteratorStackCounter extends Stat {
  var count = 1

  override def observe(sf: SimpleFeature): Unit = { }

  override def pack(): Array[Byte] = ???

  override def toJson(): String = ???

  override def add(other: Stat): Stat = {
    other match {
      case o: IteratorStackCounter => count += o.count
    }
    this
  }
}
