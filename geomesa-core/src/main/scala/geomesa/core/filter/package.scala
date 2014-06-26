package geomesa.core

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.{NotImpl, AndImpl, OrImpl, FilterAbstract}
import org.geotools.filter.visitor.DefaultFilterVisitor
import org.opengis.filter._
import scala.collection.JavaConversions._


package object filter {
  val ff = CommonFactoryFinder.getFilterFactory2

  def rewriteFilter(filter: Filter): Filter = {
    val ll =  logicDistribution(filter)

    if(ll.size == 1) {
      if(ll(0).size == 1) ll(0)(0)
      else ff.and(ll(0))
    }
    else  ff.or(ll.map(l => ff.and(l)))
  }

  def logicDistribution(x: Filter): List[List[Filter]] = x match {
    case or: OrImpl  => or.getChildren.flatMap (logicDistribution).toList

    case and: AndImpl => and.getChildren.foldRight (List(List.empty[Filter])) {
      (f, dnf) => for {
        a <- logicDistribution (f)
        b <- dnf
      } yield a ++ b
    }
    case not: NotImpl => {
      not.getFilter match {
        case and: AndImpl => logicDistribution(deMorgan(and))
        case or:  OrImpl => logicDistribution(deMorgan(or))
        case f: Filter => List(List(not))
      }
    }

    case f: Filter => List(List(f))
  }

  def deMorgan(f: Filter): Filter = f match {
    case and: AndImpl => ff.or(and.getChildren.map(a => ff.not(a)))
    case or:  OrImpl  => ff.and(or.getChildren.map(a => ff.not(a)))
    case not: NotImpl => not.getFilter
  }

}
