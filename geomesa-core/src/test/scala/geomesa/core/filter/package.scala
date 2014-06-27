package geomesa.core

import geomesa.core.filter._
import geomesa.core.filter.OrSplittingFilterTest._

package object filter {




  val a = rewriteFilter(geom1)
  val b = logicDistribution(geom1)

}
