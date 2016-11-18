package org.locationtech.geomesa.sparkgis

import java.util.ServiceLoader

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.opengis.feature.simple.SimpleFeature

trait SparkGISProvider {

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query,
          numberOfSplits: Option[Int]): RDD[SimpleFeature]

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          query: Query,
          useMock: Boolean = false,
          numberOfSplits: Option[Int] = None): RDD[SimpleFeature]

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit

}

object GeoMesaSpark {

  import scala.collection.JavaConversions._

  lazy val providers = ServiceLoader.load(classOf[SparkGISProvider])

  def apply(params: java.util.Map[String, java.io.Serializable]): SparkGISProvider =
    providers.find(_.canProcess(params)).getOrElse(throw new RuntimeException("Could not find a SparkGISProvider"))
}
