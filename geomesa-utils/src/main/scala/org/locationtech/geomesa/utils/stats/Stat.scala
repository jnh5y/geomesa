package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.combinator.RegexParsers

trait Stat {
  def observe(sf: SimpleFeature)
  def add(other: Stat): Stat

  def toJson(): String
}



object Stat {

  class StatParser extends RegexParsers {

    val attributeName = """\w+""".r

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeName <~ ")" ^^ {
        case attribute: String => new MinMax[java.lang.Long](attribute)
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorCount" ^^ { case _ => new IteratorStackCounter() }
    }

    def enumeratedHistogramParser: Parser[EnumeratedHistogram[String]] = {
      "Histogram(" ~> attributeName <~ ")" ^^ {
        case attribute: String  => new EnumeratedHistogram[String](attribute)
      }
    }

    def statParser: Parser[Stat] = {
      minMaxParser | iteratorStackParser | enumeratedHistogramParser
    }

    def statsParser: Parser[Stat] = {
      rep1sep(statParser, ",") ^^ {
        case statParsers: Seq[Stat] => new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse $s.")
      }
    }
  }

  def apply(s: String) = new StatParser().parse(s)
}
