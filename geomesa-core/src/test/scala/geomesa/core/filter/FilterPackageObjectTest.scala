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


class FilterPackageObjectTest3 extends Properties("Filters") {


  import org.scalacheck.Gen._

  val genGeom = oneOf("(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))")

  val genTopoPred = oneOf("INTERSECTS", "OVERLAPS", "WITHIN")

  val genTopoString = for {
    geom <- genGeom
    pred <- genTopoPred
  } yield s"$pred$geom"

  val genTopo: Gen[Filter] = genTopoString.map(ECQL.toFilter)

  def dt(int: Interval): String =
    s"(geomesa_index_start_time between '${int.getStart}' AND '${int.getEnd}')"

  val genTimeString =
    oneOf(IndexSchema.everywhen,
      new Interval(new DateTime("2010-07-01T00:00:00.000Z"), new DateTime("2010-07-31T00:00:00.000Z")))

  val genTime = genTimeString.map(i => ECQL.toFilter(dt(i)))

  val genAttr = Gen.choose(0, 100).map(intToAttributeFilter)

  ///val genAttr = value("attr")

  val genAtom = oneOf(genTopo, genTime, genAttr)

  val genNot  = genAtom.map(ff.not)

  val numChildren = Gen.frequency(
    (5, 2),
    (3, 3),
    (1, 4)
  )
    //.flatMap(Gen.listOfN(_, genFreq))
  
  def getChildren: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, genFreq)
  } yield c

  val pickBinary: Gen[java.util.List[Filter] => Filter] = oneOf(Seq[java.util.List[Filter] => Filter](ff.or, ff.and))

  val genBinary: Gen[Filter] = for {
    l <- getChildren
    b <- pickBinary
  } yield { b(l) }


  //def genBaseFilter = oneOf(genNot, genTopo, genTemp, genAttr)

  def genBaseFilter: Gen[Filter] = Gen.frequency(
    (2, genTopo),
    (2, genTime),
    (1, genAttr),
    (1, genNot)
  )

  def genFreq: Gen[Filter] = Gen.frequency(
    (1, genBinary),
    (2, genBaseFilter)
  )
  
}