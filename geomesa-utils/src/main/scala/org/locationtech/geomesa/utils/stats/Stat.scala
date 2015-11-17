package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.combinator.RegexParsers

trait Stat {
  def observe(sf: SimpleFeature)
  def add(other: Stat): Stat

  def toJson(): String

  def pack(): Array[Byte]
}



object Stat {

  class StatParser extends RegexParsers {

    val attributeName = "[^(),]*".r

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeName <~ ")" ^^ {
        case attribute: String => new MinMax[java.lang.Long](attribute)
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorCount" ^^ { case _ => new IteratorStackCounter() }
    }

    def statParser: Parser[Stat] = {
      minMaxParser | iteratorStackParser
    }

    def statsParser: Parser[Seq[Stat]] = {
      rep1sep(statParser, ",")
    }

    def parse(s: String): Seq[Stat] = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse $s.")
      }
    }
  }

  def apply(s: String) = new StatParser().parse(s)
}
