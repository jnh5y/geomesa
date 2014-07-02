package geomesa.core.filter

import geomesa.core.index.IndexSchema
import geomesa.utils.geohash.BoundingBox
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, Interval}
import org.opengis.filter.{BinaryLogicOperator, Filter, Or}
import org.scalacheck.Gen
import org.scalacheck.Gen._
import scala.collection.JavaConversions._

object FilterGenerator {

  val ff = CommonFactoryFinder.getFilterFactory2

  implicit class RichFilter(val filter: Filter) {
    def &&(that: RichFilter) = ff.and(filter, that.filter)

    def ||(that: RichFilter) = ff.or(filter, that.filter)

    def &&(that: Filter) = ff.and(filter, that)

    def ||(that: Filter) = ff.or(filter, that)

    def ! = ff.not(filter)
  }

  implicit def stringToFilter(s: String) = ECQL.toFilter(s)

  def intToAttributeFilter(i: Int): Filter = s"attr$i = val$i"

  implicit def intToFilter(i: Int): RichFilter = intToAttributeFilter(i)

  val maxLon = 50
  val minLon = 40
  val maxLat = 30
  val minLat = 20

  def buildRange(min: Int, max: Int): Gen[(Int, Int)] = Gen.pick(2, min to max).map(a=> (a(0), a(1)))

  def genBBOX(): Gen[BoundingBox] = {
    val lat = buildRange(minLat, maxLat)
    val lon = buildRange(minLon, maxLon)

    val g = for {
      (minX, maxX) <- lon
      (minY, maxY) <- lat
    } yield BoundingBox(minX, maxX, minY, maxY)

    g
  }

  def genGeom = oneOf("(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))")

  def genTopoPred = Gen.const("INTERSECTS") //oneOf("INTERSECTS", "OVERLAPS", "WITHIN")

  def genTopoString = for {
    geom <- genGeom
    pred <- genTopoPred
  } yield s"$pred$geom"

  def genTopo: Gen[Filter] = genTopoString.map(ECQL.toFilter)

  def dt(int: Interval): String =
    s"(geomesa_index_start_time between '${int.getStart}' AND '${int.getEnd}')"

  def genTimeString =
    oneOf(IndexSchema.everywhen,
      new Interval(new DateTime("2010-07-01T00:00:00.000Z"), new DateTime("2010-07-31T00:00:00.000Z")))

  def genTime = genTimeString.map(i => ECQL.toFilter(dt(i)))

  def genAttr = Gen.choose(0, 100).map(intToAttributeFilter)

  def genAtom = oneOf(genTopo, genTime, genAttr)

  def genNot  = genAtom.map(ff.not)

  def numChildren: Gen[Int] = Gen.frequency(
    (5, 2),
    (3, 3),
    (1, 4)
  )

  def getChildren: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, genFreq)
  } yield c

  def pickBinary: Gen[java.util.List[Filter] => Filter] = oneOf(Seq[java.util.List[Filter] => Filter](ff.or, ff.and))

  val genBinary: Gen[Filter] = for {
    l <- getChildren
    b <- pickBinary
  } yield { b(l) }


  val genBaseFilter: Gen[Filter] = Gen.frequency(
    (2, genTopo),
    (2, genTime),
    (1, genAttr),
    (1, genNot)
  )

  val genFreq: Gen[Filter] = Gen.frequency(
    (2, genBinary),
    (3, genBaseFilter)
  )

  def getChildrenPositive: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, genFreqPositive)
  } yield c


  val genBinaryPositive: Gen[Filter] = for {
    l <- getChildrenPositive
    b <- pickBinary
  } yield { b(l) }

  val genBaseFilterPositive: Gen[Filter] = Gen.frequency(
    (2, genTopo),
    (2, genTime),
    (1, genAttr)
  )

  val genFreqPositive: Gen[Filter] = Gen.frequency(
    (2, genBinaryPositive),
    (3, genBaseFilterPositive)
  )

  def getChildren2: Gen[List[Filter]] = for {
    n <- numChildren
    c <- Gen.listOfN(n, oneOf(genTopo, genAttr))
  } yield c

  // genTime can return filters for Intervals which have 'AND's.
  def genOneLevelBinary[T](f: java.util.List[Filter] => T): Gen[T] =
    getChildren2.map(l => f(l))

  def genOneLevelOr = genOneLevelBinary(ff.or)
  def genOneLevelAnd = genOneLevelBinary(ff.and)
  def genOneLevelAndOr = oneOf(genOneLevelAnd, genOneLevelOr)

  def runSamples[T](gen: Gen[T])(thunk: T => Any) = {
    (0 until 20).foreach { _ => gen.sample.map(thunk) }
  }
}

object FilterUtils {
  def decomposeBinary(f: Filter): Seq[Filter] = {
    f match {
      case b: BinaryLogicOperator => b.getChildren.toSeq.flatMap(decomposeBinary)
      case f: Filter => Seq(f)
    }
  }

  def decomposeOr(f: Filter): Seq[Filter] = {
    f match {
      case b: Or => b.getChildren.toSeq.flatMap(decomposeOr)
      case f: Filter => Seq(f)
    }
  }
}