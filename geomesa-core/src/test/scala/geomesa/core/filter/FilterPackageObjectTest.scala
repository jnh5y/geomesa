package geomesa.core.filter

import geomesa.core.filter.OrSplittingFilterTest._
import geomesa.core.index.IndexSchema
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, Interval}
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Properties, Prop, Gen}
import org.specs2.mutable.Specification
import geomesa.core.filter.FilterGenerator._
import org.specs2.runner.JUnitRunner
import org.opengis.filter._
import org.specs2.specification.Fragments
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FilterPackageObjectTest extends Specification {

  // Test deMorgan
  def testDeMorgan(filter: Filter) = "The deMorgan function" should {

    "change ANDs to ORs" in {

    }

    "change ORs to ANDs" in {

    }

    "be idempotent" in {

    }

    "not affect filters without binary operators" in {

    }

    "remove stacked NOTs" in {

    }

  }


  // Test logicDistribution
  "The function 'logicDistribution'" should {
    "split a top-level OR into a List of single-element Lists each containing a filter" in {


    }

    "split a top-level AND into a a singleton List which contains a List of the ANDed filters" in {

    }

    "not return filters with ANDs or ORs explicitly stated" in {
      // NB: The nested lists imply ANDs and ORs.
      // Property-check.

    }

    "take a 'simple' filter and return List(List(filter))" in {

    }




  }


  // Function defining rewriteFilter Properties.
  def testRewriteProps(filter: Filter) = {
    "The function rewriteFilter" should {

      val rewrittenFilter = rewriteFilter(filter)

      "return a Filter with at most one OR" in {


      }

      "return a Filter where the children of the (optional) OR can (optionally) be an AND" in {

      }

      "return a Filter where NOTs do not have ANDs or ORs as children" in {

      }

      "return a Filter which is 'equivalent' to the original filter" in {

      }

    }
    true
  }


  def test(i: Int) = {
    "The function" should {
      "add blah" in {
        println("TESTING")
        i mustEqual i
      }
    }
  }

  def testRewriteProps2(filter: Filter) = {
    //"The function rewriteFilter" should {

    val rewrittenFilter = rewriteFilter(filter)

    //"return a Filter with at most one OR" in {
    def checkStructure1(): Boolean = true

    //"return a Filter where the children of the (optional) OR can (optionally) be an AND" in {
    def checkStructure2(): Boolean = true


    //"return a Filter where NOTs do not have ANDs or ORs as children" in {
    def checkStructure3(): Boolean = true

    // "return a Filter which is 'equivalent' to the original filter" in {
    def checkEquivalent(): Boolean = true


    checkStructure1 && checkStructure2 && checkStructure3 && checkEquivalent()
  }

//  def genFilter: Gen[Filter] = ???
//
//  implicit val genFilterImplicit = Arbitrary { genFilter }
//
//
//  Prop.forAll { f: Filter => { testRewriteProps(f) && testRewriteProps2(f) }  }


  val l = (geom1 || date1).! && 1
  val r = (geom2 && 2).! || 6
  val tree = ff.and(l, r)

  rewriteFilter(l)

  rewriteFilter(r)

  rewriteFilter(tree)

}

class Quick extends Specification {
  def test(i: Int) = {
    "The function" should {
      "add blah" in {
        println("TESTING")
        i mustEqual i
      }
    }
  }
  test(1)
}

@RunWith(classOf[JUnitRunner])
class FilterPackageObjectTest2 extends Properties("Filters") {
  //val a = implicitly(JUnitRunner)

  import Prop.forAll

  property("1") = forAll { (a: String) => true }

}

