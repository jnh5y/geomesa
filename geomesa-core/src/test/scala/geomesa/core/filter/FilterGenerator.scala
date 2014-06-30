package geomesa.core.filter

import geomesa.utils.geohash.BoundingBox
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.opengis.filter.Filter
import org.scalacheck.Gen
import scala.collection.immutable.NumericRange
import scala.runtime.RangedProxy

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

  def br[T](min: RangedProxy[T], max: T)(implicit ev: RangedProxy[T]#ResultWithoutStep <:< NumericRange[T]) = Gen.pick(2, min to max)

  //type NR = A forSome { type T :: type RangedProxy[T]#ResultWithoutStep <: Seq }


  type A = B forSome { type B <: RangedProxy[B] }

  // Generate filters via ScalaCheck.

  //def buildRange[T forSome { type T}](min: RangedProxy[T], max: T)(implicit num: RangedProxy[T]#ResultWithoutStep <:< NumericRange[T]) = Gen.pick(2, min to max)

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

}
