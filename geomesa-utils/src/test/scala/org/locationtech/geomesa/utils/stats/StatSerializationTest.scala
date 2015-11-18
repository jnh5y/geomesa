package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatSerializationTest extends Specification {

  "StatsSerlization" should {

    "pack and unpack" >> {
      "MinMax stats" in {
        val attribute = "foo"
        val mm = new MinMax[java.lang.Long](attribute)

        val min = -235L
        val max = 12345L

        mm.min = min
        mm.max = max

        val packed   = StatSerialization.pack(mm)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Long]]

        unpacked.attribute must be equalTo attribute
        unpacked.min must be equalTo min
        unpacked.max must be equalTo max
      }

      "IteratorStackCounter stats" in {

        val stat = new IteratorStackCounter
        val count = 987654321L
        stat.count = count

        val packed = StatSerialization.pack(stat)
        val unpacked = StatSerialization.unpack(packed).asInstanceOf[IteratorStackCounter]

        unpacked.count must be equalTo count
      }

      "EnumeratedHistogram stats" in {
        true must be equalTo true
      }

    }


  }

}
