package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatTest extends Specification  {

  //val sft = SimpleFeatureTypes.createType()

  "DSL" should {
    "create MinMax stat gather" in {
      val stats = Stat("MinMax(foo)")
      val stat  = stats.asInstanceOf[SeqStat].stats.head

      val mm = stat.asInstanceOf[MinMax[java.lang.Long]]
      mm.attribute mustEqual "foo"
    }

    "create a Sequence of Stats" in {
      val stat = Stat("MinMax(foo),MinMax(bar),IteratorCount")

      val stats = stat.asInstanceOf[SeqStat].stats

      stats.size mustEqual 3

      stats(0).asInstanceOf[MinMax[java.lang.Long]].attribute mustEqual "foo"
      stats(1).asInstanceOf[MinMax[java.lang.Long]].attribute mustEqual "bar"

      stats(2) must beAnInstanceOf[IteratorStackCounter]
    }

  }


}
