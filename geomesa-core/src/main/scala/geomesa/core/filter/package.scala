package geomesa.core

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.{NotImpl, AndImpl, OrImpl, FilterAbstract}
import org.geotools.filter.visitor.DefaultFilterVisitor
import org.opengis.filter._
import scala.collection.JavaConversions._


package object filter {
  val ff2 = CommonFactoryFinder.getFilterFactory2

  def rewriteFilter(filter: Filter): Filter = {
    val ll =  logicDistribution(filter)

    if(ll.size == 1) {
      if(ll(0).size == 1) ll(0)(0)
      else ff2.and(ll(0))
    }
    else  ff2.or(ll.map(l => ff2.and(l)))
  }

  def logicDistribution(x: Filter): List[List[Filter]] = x match {
    case or: OrImpl  => or.getChildren.flatMap(logicDistribution).toList

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
    case and: AndImpl => ff2.or(and.getChildren.map(a => ff2.not(a)))
    case or:  OrImpl  => ff2.and(or.getChildren.map(a => ff2.not(a)))
    case not: NotImpl => not.getFilter
  }

  val ex: Filter = Filter.EXCLUDE

  def ld2(x: Filter): Filter = x match {

    case or: OrImpl  => ff2.and(or.getChildren.map(ld2))

    case and: AndImpl => {
      val ands = and.getChildren.map(ld2)

      ands.combinations(2).map { b =>
        ff2.and(b(0), b(1))
      }.toList

      ff2.or(ands)
//      val a = and.getChildren.foldRight( List[Filter](Filter.INCLUDE) ) {
//        (f, dnf) => for {
//          b <- dnf
//          a = ld2(f)
//        } yield ff2.and(a, b)
//      }
//
//      ff2.or(a)
    }
    case not: NotImpl => {
      not.getFilter match {
        case and: AndImpl => ld2(deMorgan(and))
        case or:  OrImpl => ld2(deMorgan(or))
        case f: Filter => not
      }
    }

    case f: Filter => f
  }

}
