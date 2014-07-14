package geomesa.core.filter

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStoreTest}
import geomesa.core.filter.FilterUtils._
import geomesa.core.filter.TestFilters._
import geomesa.core.iterators.TestData._
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Fragments
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import geomesa.core.filter.FilterUtils._


@RunWith(classOf[JUnitRunner])
class AllPredicateTest extends FilterTester {
  val filters = allPreds
}

@RunWith(classOf[JUnitRunner])
class AndGeomsPredicateTest extends FilterTester {
  val filters = andGeoms
}

@RunWith(classOf[JUnitRunner])
class OrGeomsPredicateTest extends FilterTester {
  val filters = orGeoms
}

trait FilterTester extends AccumuloDataStoreTest with Logging {

  val mediumDataFeatures: Seq[SimpleFeature] = mediumData.map(createSF)
  val sft = mediumDataFeatures.head.getFeatureType

  def filters: Seq[String]
  
  val ds = createStore
  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]
  val coll = new DefaultFeatureCollection(sft.getTypeName)
  coll.addAll(mediumDataFeatures.asJavaCollection)

  logger.debug("Adding SimpleFeatures to feature store.")
  fs.addFeatures(coll)
  logger.debug("Done adding SimpleFeaturest to feature store.")
  

  def compareFilter(filter: Filter): Fragments = {
    logger.debug(s"Filter: ${ECQL.toCQL(filter)}")

    s"The filter $filter" should {

      "return the same number of results from filtering and querying" in {
        val filterCount = mediumDataFeatures.count(filter.evaluate)
        val queryCount = fs.getFeatures(filter).size
        
        logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${mediumDataFeatures.size}: " +
          s"filter hits: $filterCount query hits: $queryCount")
        filterCount mustEqual queryCount
        filterCount mustNotEqual 0
      }
    }
  }

  filters.foreach {s => compareFilter(s) }

}
