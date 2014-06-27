import geomesa.core.filter.OrSplittingFilterTest._
import geomesa.core.filter._
import org.geotools.factory.CommonFactoryFinder
import org.opengis.filter._
import scala.collection.JavaConversions._

val ff2 = CommonFactoryFinder.getFilterFactory2


val l = (geom1 || date1).! && 1
val r = (geom2 && 2).! || 6

val t = ff.and(l, r)

def rewriteFilter(filter: Filter): Filter = {
  val ll =  logicDistribution(filter)

  if(ll.size == 1) {
    if(ll(0).size == 1) ll(0)(0)
    else ff2.and(ll(0))
  }
  else  ff2.or(ll.map(l => ff2.and(l)))
}

/**
 *
 * @param x: A general
 * @return
 */
def logicDistribution(x: Filter): List[List[Filter]] = x match {
  case or: Or  => or.getChildren.flatMap(logicDistribution).toList

  case and: And => and.getChildren.foldRight (List(List.empty[Filter])) {
    (f, dnf) => for {
      a <- logicDistribution (f)
      b <- dnf
    } yield a ++ b
  }
  case not: Not => {
    not.getFilter match {
      case and: And => logicDistribution(deMorgan(and))
      case or:  Or => logicDistribution(deMorgan(or))
      case f: Filter => List(List(not))
    }
  }

  case f: Filter => List(List(f))
}

/**
 *  The input is a filter which had a Not applied to it.
 *  This function uses deMorgan's law to 'push the Not down'
 *   as well as cancel adjacent Nots.
 */
def deMorgan(f: Filter): Filter = f match {
  case and: And => ff2.or(and.getChildren.map(a => ff2.not(a)))
  case or:  Or  => ff2.and(or.getChildren.map(a => ff2.not(a)))
  case not: Not => not.getFilter
}

def ld2(x: Filter): Filter = x match {

  case or: Or  => ff2.or(or.getChildren.map(ld2))

  case and: And => {
    val ands = and.getChildren.map(ld2)

    ands.combinations(2).map { b =>
      ff2.and(b(0), b(1))
    }.toList

    ff2.and(ands)
    //      val a = and.getChildren.foldRight( List[Filter](Filter.INCLUDE) ) {
    //        (f, dnf) => for {
    //          b <- dnf
    //          a = ld2(f)
    //        } yield ff2.and(a, b)
    //      }
    //
    //      ff2.or(a)
  }
  case not: Not => {
    not.getFilter match {
      case and: And => ld2(deMorgan(and))
      case or:  Or => ld2(deMorgan(or))
      case f: Filter => not
    }
  }

  case f: Filter => f
}


rewriteFilter(l)

rewriteFilter(r)

ld2(l)

ld2(r)

rewriteFilter(t)

ld2(t)

t.getChildren.map(ld2).foreach{println}