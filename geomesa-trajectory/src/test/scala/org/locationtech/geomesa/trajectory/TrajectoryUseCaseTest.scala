package org.locationtech.geomesa.trajectory

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.commons.csv.CSVFormat
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.{DefaultCounter, SimpleFeatureConverters}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TrajectoryUseCaseTest extends Specification {

  val conf = ConfigFactory.load("reference.conf")
  val sft = conf.root().render(ConfigRenderOptions.concise())
  val converterConfig = conf.root().render(ConfigRenderOptions.concise())

  // Read the data
  import scala.collection.JavaConversions._
  val data = Resources.readLines(Resources.getResource("2237.txt"), StandardCharsets.UTF_8)

  "We must read input data " >> {

    val converter = SimpleFeatureConverters.build[String]("tdrive", "tdrive")
    converter must not beNull
    val res = converter.processInput(data.iterator()).toList
    converter.close()

  }

}
