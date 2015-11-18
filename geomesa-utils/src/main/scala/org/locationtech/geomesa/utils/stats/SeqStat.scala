package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

class SeqStat(val stats: Seq[Stat]) extends Stat {
  override def observe(sf: SimpleFeature): Unit = stats.foreach(_.observe(sf))

  // Pack will require specifying a special Byte/Byte Sequence to indicate how to
  // handle serialization between different types.
  override def pack(): Array[Byte] = ???

  // JNH: Does this work?  Or is it too quickly?
  override def toJson(): String = stats.map(_.toJson()).mkString(",")

  // JNH: revisit this.  Might need to deal with 'empty' stats or more bizarre situations
  override def add(other: Stat): Stat = {
    other match {
      case ss: SeqStat =>
        stats.zip(ss.stats).foreach { case (stat1, stat2) => stat1.add(stat2) }
    }
    this
  }
}
