package geomesa.core.filter

import geomesa.core.filter.OrSplittingFilterTest._
import geomesa.core.index.IndexSchema
import org.geotools.filter.text.ecql.ECQL
import geomesa.core.filter.FilterUtils._
import org.joda.time.{DateTime, Interval}
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Properties, Prop, Gen}
import org.specs2.mutable.Specification
import geomesa.core.filter.FilterGenerator._
import org.specs2.runner.JUnitRunner
import org.opengis.filter._
import org.specs2.specification.Fragments
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.slf4j.Logging
import org.scalacheck.Prop._

import geomesa.core.iterators.TestData._

@RunWith(classOf[JUnitRunner])
class FilterPackageObjectTest extends Specification with Logging {

  // Test deMorgan
  //def testDeMorgan(filter: Filter) =
  "The deMorgan function" should {

    "change ANDs to ORs" in {
      runSamples(genOneLevelAnd) { f =>
        val dm = deMorgan(f)
        dm.isInstanceOf[Or] mustEqual true
        val dmChildren = dm.asInstanceOf[Or].getChildren

        f.getChildren.zip(dmChildren).map {
          case (origChild, dmChild) =>
            dmChild.isInstanceOf[Not] mustEqual true
            dmChild.asInstanceOf[Not].getFilter mustEqual(origChild)
        }
      }

    }

    "change ORs to ANDs" in {
      runSamples(genOneLevelOr) { f =>
        val dm = deMorgan(f)
        dm.isInstanceOf[And] mustEqual true
        val dmChildren = dm.asInstanceOf[And].getChildren

        f.getChildren.zip(dmChildren).map {
          case (origChild, dmChild) =>
            dmChild.isInstanceOf[Not] mustEqual true
            dmChild.asInstanceOf[Not].getFilter mustEqual(origChild)
        }
      }
    }

    "be idempotent" in {
      runSamples(genFreq) { f =>
        val dmf = deMorgan(f)
        val dmdmf = deMorgan(dmf)
        dmf mustEqual dmdmf
      }
    }

    "not affect filters without binary operators" in {
      runSamples(genBaseFilter) { f =>
        f mustEqual deMorgan(f)
      }

    }

    "remove stacked NOTs" in {
      runSamples(genNot) { f =>
        deMorgan(ff.not(f)) mustEqual f.getFilter
      }
    }
  }


  // Test logicDistribution
  "The function 'logicDistribution'" should {


    "split a top-level OR into a List of single-element Lists each containing a filter" in {
      runSamples(genOneLevelOr) { or =>
        val ll = logicDistribution(or)
        ll.foreach { l => l.size mustEqual 1 }
      }
    }

    "split a top-level AND into a a singleton List which contains a List of the ANDed filters" in {

      runSamples(genOneLevelAnd) { and =>
        val ll = logicDistribution(and)
        ll.size mustEqual 1
        and.getChildren.size mustEqual ll(0).size
      }

    }

    "not return filters with ANDs or ORs explicitly stated" in {
      // NB: The nested lists imply ANDs and ORs.
      // Property-check.
      runSamples(genFreq) { filter: Filter =>
        val ll = logicDistribution(filter)
        ll.flatten.foreach { l => l.isInstanceOf[BinaryLogicOperator] mustEqual false }
      }
    }

    "take a 'simple' filter and return List(List(filter))" in {
      runSamples(genBaseFilter) { f =>
        val ll = logicDistribution(f)
        ll.size mustEqual 0
        ll(0).size mustEqual 0
      }
    }

  }


  // Function defining rewriteFilter Properties.
  def testRewriteProps(filter: Filter) = {
    logger.debug(s"Filter: $filter")

    "The function rewriteFilter" should {

      val rewrittenFilter: Filter = rewriteFilter(filter)

      logger.debug(s"Rewritten as $rewrittenFilter")

      "return a Filter with at most one OR at the top" in {
        val decomp = decomposeBinary(rewrittenFilter)

        val orCount = decomp.count(_.isInstanceOf[Or])

        orCount mustEqual 0
      }

      val children = decomposeOr(rewrittenFilter)

      "return a Filter where the children of the (optional) OR can (optionally) be an AND" in {
        children.map { _.isInstanceOf[Or] mustEqual false}
      }

      "return a Filter where NOTs do not have ANDs or ORs as children" in {
        children.filter(_.isInstanceOf[Not]).foreach { f =>
          f.isInstanceOf[BinaryLogicOperator] mustEqual false
        }
      }

      "return a Filter which is 'equivalent' to the original filter" in {
        fullData.filter(rewrittenFilter.evaluate) mustEqual fullData.filter(filter.evaluate)
      }

    }
  }

  val g = genFreq
  (0 until 10).foreach { i =>
    logger.debug(s"Running test $i")
    g.sample.map(testRewriteProps)
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

