package geomesa.core.filter

import geomesa.core.filter.OrSplittingFilterTest._
import org.specs2.mutable.Specification
import geomesa.core.filter.FilterGenerator._

class FilterPackageObjectTest extends Specification {

  // Test deMorgan




  // Test logicDistribution

  // Test rewriteFilter

  val l = (geom1 || date1).! && 1
  val r = (geom2 && 2).! || 6
  val tree = ff.and(l, r)

  rewriteFilter(l)

  rewriteFilter(r)

  rewriteFilter(tree)

}
