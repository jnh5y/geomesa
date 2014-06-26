package geomesa.core.filter

import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FilterAbstract
import org.geotools.filter.visitor.DefaultFilterVisitor
import org.opengis.filter._
import scala.collection.JavaConversions._

class SubtreeTypingFilter extends DefaultFilterVisitor {

  val ff = CommonFactoryFinder.getFilterFactory2

  def visit(filter: Filter): Any = {
    println(s"Visiting $filter")

    filter match {
      case _ : IncludeFilter =>    println("Include")
      case _ : Id             =>   println("ID")
      case _ : ExcludeFilter  =>   println("EXCLUDE")
      case _ : PropertyIsNil  =>   println("NIL")
      case _ : PropertyIsNull =>   println("NULL")
      case bin : BinaryLogicOperator => println("Binary: Visiting children")
        bin.getChildren.foreach { c => visit(c) }

      case _ : MultiValuedFilter  => println("Multivalued")
      case _ : Not =>         println("NOT")

      case _ : org.geotools.filter.Filter         =>   println("GT Filter")
      case _ : FilterAbstract =>   println("Abstract GT filter")

      case _     =>          println("UNKNOWN")
    }
  }

  def translateOr(or: Or): Filter = {
    def flip(f: Filter) = f match {
      case n: Not => n.getFilter
      case f: Filter => ff.not(f)
    }

    val fs = for {
      child <- or.getChildren
    } yield {
      flip(child)
    }

    ff.and(fs)
  }
}


sealed trait F2 {
  def &&(that: F2) = And(List(this, that))
  def ||(that: F2) = Or(List(this, that))
}

case class Atom(s: String) extends F2
case class And(l: List[F2]) extends F2
case class Or(l: List[F2]) extends F2
case class Not(f: F2) extends F2

object F2 {
  implicit def strToAtom(s: String): Atom = Atom(s)
  implicit def intToAtom(i: Int): Atom = Atom(i.toString)
  type DNF = List[List[Atom]]

  def logicDistribution(x: F2): DNF = x match {
    case a: Atom => List(List(a))
    case Or(l)  => l.flatMap (logicDistribution)
    case And(l) => l.foldRight (List(List.empty[Atom])) {
      (f, dnf) => for {
        a <- logicDistribution (f)
        b <- dnf
      } yield a ++ b
    }
    case v@Not(Atom(a)) => List(List(s"NOT$a"))
    case Not(f) => logicDistribution(deMorgan(f))
  }

  def deMorgan(f: F2): F2 = f match {
    case a: Atom => a
    case And(l) => Or(l.map(a => Not(a)))
    case Or(l)  => And(l.map(a => Not(a)))
    case Not(a) => a
  }
}


class RString(s: String) {
  implicit def strToAtom(s: String): Atom = Atom(s)
  def &&(that: F2) = s.&&(that)
  def ||(that: F2) = s.||(that)
}

class RInt(i: Int) {
  implicit def intToAtom(i: Int): Atom = Atom(i.toString)
  def &&(that: F2) = i.&&(that)
  def ||(that: F2) = i.||(that)
}