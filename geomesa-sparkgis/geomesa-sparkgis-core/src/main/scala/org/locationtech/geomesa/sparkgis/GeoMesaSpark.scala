package org.locationtech.geomesa.sparkgis

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.Query
import org.opengis.feature.simple.SimpleFeature

/**
  * Created by afox on 11/18/16.
  */
trait GeoMesaSpark {

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

object GeoMesaSpark extends GeoMesaSpark {
  override def rdd(conf: Configuration, sc: SparkContext, dsParams: Map[String, String], query: Query, numberOfSplits: Option[Int]): RDD[SimpleFeature] = ???

  override def rdd(conf: Configuration, sc: SparkContext, dsParams: Map[String, String], query: Query, useMock: Boolean, numberOfSplits: Option[Int]): RDD[SimpleFeature] = ???

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  override def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = ???
}

