package org.locationtech.geomesa.jobs.raster

import java.awt.image.RenderedImage
import javax.media.jai.{Histogram, JAI}

import com.twitter.scalding._
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.feature.SimpleFeatureDecoder
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.jobs.scalding._
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.raster.index.RasterIndexSchema
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

trait RasterJobResources {
  def ds: AccumuloDataStore
  def sft: SimpleFeatureType
  def visibilities: String
  def decoder: SimpleFeatureDecoder
  def attributeDescriptors: mutable.Buffer[(Int, AttributeDescriptor)]

  // required by scalding
  def release(): Unit = {}
}

object RasterJobResources {
  import scala.collection.JavaConversions._
  def apply(params:  Map[String, String], feature: String, attributes: List[String]) = new RasterJobResources {
    val ds: AccumuloDataStore = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
    val sft: SimpleFeatureType = ds.getSchema(feature)
    val visibilities: String = ds.writeVisibilities
    val decoder: SimpleFeatureDecoder = SimpleFeatureDecoder(sft, ds.getFeatureEncoding(sft))
    // the attributes we want to index
    override val attributeDescriptors =
      sft.getAttributeDescriptors
        .zipWithIndex
        .filter { case (ad, idx) => attributes.contains(ad.getLocalName) }
        .map { case (ad, idx) => (idx, ad) }
  }
}

class SampleRasterJobGrayScale(args: Args) extends Job(args) {

  //  lazy val zookeepers       = args(ConnectionParams.ZOOKEEPERS)
  //  lazy val instance         = args(ConnectionParams.ACCUMULO_INSTANCE)
  //  lazy val user             = args(ConnectionParams.ACCUMULO_USER)
  //  lazy val password         = args(ConnectionParams.ACCUMULO_PASSWORD)
  //lazy val tablename        = args(ConnectionParams.CATALOG_TABLE)

  //val tablename = "AANNEX_SRI_ALL_VIS_RASTERS"

  val inputTable = "jnh_color"
  val outputTable = "jnh_grayscale"
  //lazy val input   = AccumuloInputOptions(inputTable, authorizations = new Authorizations("S", "USA"))
  lazy val input   = AccumuloInputOptions(inputTable)
  lazy val output  = AccumuloOutputOptions(outputTable)
  lazy val options = AccumuloSourceOptions("dcloud", "dzoo1", "root", "secret", input, output)

  //println(s"Args decode to $zookeepers $instance $user $password $inputTable")

  //  AccumuloSource(options)
  //    .map(('key, 'value) -> ('line, 'number)) {
  //    (kv: (Key, Value)) => ("foo", 2)
  //  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output"))

  //  AccumuloSource(options)
  //    .mapTo('line) {
  //      (kv: (Key, Value)) => s"rowId ${kv._1.getRow}"
  //  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output"))

  //  AccumuloSource(options)
  //    .mapTo('mutation) {
  //    (kv: (Key, Value)) => SampleRasterJob.kvToMutation(kv._1, kv._2)
  //  }.write(AccumuloSource(options))
  //
  //  // Working Grayscale code
    AccumuloSource(options)
      .map(('key, 'value) -> 'mutation) {
        (kv: (Key, Value)) => {
          GrayscaleJob.colorKVtoGrayScaleMutation(kv._1, kv._2)
        }
    }.write(AccumuloSource(options))

  // Working Grayscale code
//  val a =   AccumuloSource(options)
//    .mapTo[Array[Int]]('hist) {
//    (kv: (Key, Value)) => {
//      HistogramJob.kvToHistogram(kv._1, kv._2)
//    }
//  }.groupAll {
//    _.reduce[Array[Int]]('hist -> 'totalHist) {
//      (h1: Array[Int], h2: Array[Int]) => HistogramJob.addBins(h1, h2)
//    }
//  }.mapTo[Array[Int], String]('totalHist -> 'line) {
//    (array: Array[Int]) => s"Histogram = Array(${array.mkString(",")})"
//  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output/histogram"))

  //    .groupAll { _.reduce[Array[Int]]('hist -> 'totHist) {
  //    (h1: Array[Int], h2: Array[Int]) => HistogramJob.addBins(h1, h2)
  //  }
  //  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output/histogram"))

}

class SampleRasterJobHistogram(args: Args) extends Job(args) {

//  lazy val zookeepers       = args(ConnectionParams.ZOOKEEPERS)
//  lazy val instance         = args(ConnectionParams.ACCUMULO_INSTANCE)
//  lazy val user             = args(ConnectionParams.ACCUMULO_USER)
//  lazy val password         = args(ConnectionParams.ACCUMULO_PASSWORD)
  //lazy val tablename        = args(ConnectionParams.CATALOG_TABLE)

  //val tablename = "AANNEX_SRI_ALL_VIS_RASTERS"

  val inputTable = "jnh_color"
  val outputTable = "jnh_grayscale"
  //lazy val input   = AccumuloInputOptions(inputTable, authorizations = new Authorizations("S", "USA"))
  lazy val input   = AccumuloInputOptions(inputTable)
  lazy val output  = AccumuloOutputOptions(outputTable)
  lazy val options = AccumuloSourceOptions("dcloud", "dzoo1", "root", "secret", input, output)

  //println(s"Args decode to $zookeepers $instance $user $password $inputTable")

//  AccumuloSource(options)
//    .map(('key, 'value) -> ('line, 'number)) {
//    (kv: (Key, Value)) => ("foo", 2)
//  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output"))

//  AccumuloSource(options)
//    .mapTo('line) {
//      (kv: (Key, Value)) => s"rowId ${kv._1.getRow}"
//  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output"))

//  AccumuloSource(options)
//    .mapTo('mutation) {
//    (kv: (Key, Value)) => SampleRasterJob.kvToMutation(kv._1, kv._2)
//  }.write(AccumuloSource(options))
//
////  // Working Grayscale code
//  AccumuloSource(options)
//    .map(('key, 'value) -> 'mutation) {
//      (kv: (Key, Value)) => {
//        GrayscaleJob.colorKVtoGrayScaleMutation(kv._1, kv._2)
//      }
//  }.write(AccumuloSource(options))

  // Working Grayscale code
 val a =   AccumuloSource(options)
    .mapTo[Array[Int]]('hist) {
      (kv: (Key, Value)) => {
        HistogramJob.kvToHistogram(kv._1, kv._2)
      }
  }.groupAll {
    _.reduce[Array[Int]]('hist -> 'totalHist) {
      (h1: Array[Int], h2: Array[Int]) => HistogramJob.addBins(h1, h2)
    }
  }.mapTo[Array[Int], String]('totalHist -> 'line) {
    (array: Array[Int]) => s"Histogram = Array(${array.mkString(",")})"
  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output/histogram"))

//    .groupAll { _.reduce[Array[Int]]('hist -> 'totHist) {
//    (h1: Array[Int], h2: Array[Int]) => HistogramJob.addBins(h1, h2)
//  }
//  }.write(TextLine("hdfs://dhead:54310/tmp/jnh-test/output/histogram"))

}

object RasterJobs {
  val schema = RasterIndexSchema("")
}

import org.locationtech.geomesa.jobs.raster.RasterJobs._

object GrayscaleJob {
  val d = Array(Array(.21, .71, 0.07, 0.0))

  def colorKVtoGrayScaleMutation(k: Key, v: Value): Mutation = {
    val raster = schema.decode(k, v)

    import javax.media.jai._
    val grayScale: RenderedImage = JAI.create("bandcombine", raster.chunk, d, null)

    val env = raster.metadata.geom.getEnvelopeInternal

    //val realRes = (env.getMaxX - env.getMinX) / raster.chunk.getWidth
    val grayRaster = Raster(grayScale, raster.metadata, raster.resolution)

    val (nk, nv) = schema.encode(grayRaster)

    //val grayBytes = Raster.encodeToBytes(grayRaster)

    val m = new Mutation(k.getRow)
    m.put(nk.getColumnFamily, nk.getColumnQualifier, nk.getColumnVisibilityParsed, nv)
    m
  }

}

object HistogramJob {

  def kvToHistogram(k: Key, v: Value): Array[Int] = {
    val raster = schema.decode(k, v)
    getHist(raster.chunk)
  }

  def getHist(image: java.awt.image.RenderedImage): Array[Int] = {
    val dst = JAI.create("histogram", image, null)
    val h = dst.getProperty("histogram").asInstanceOf[Histogram]
    h.getBins(0)
  }

  def addBins[T : Numeric : ClassTag](a: Array[T], b: Array[T]): Array[T] = {
    require(a.length == b.length)
    val op = implicitly[Numeric[T]]

    val ret = new Array[T](a.length)

    var i = 0

    while(i < a.length) {
      ret(i) = op.plus(a(i), b(i))
      i += 1
    }
    ret
  }

}

object SampleRasterJobGrayScale {

  def kvToMutation(k: Key, v: Value): Mutation = {
    val m = new Mutation(k.getRow)

    m.put(k.getColumnFamily, k.getColumnQualifier, k.getColumnVisibilityParsed, v)
    m
  }

  val conf = new Configuration
  conf.set("accumulo.monitor.address", "damaster"
  )
  conf.set("mapreduce.framework.name", "yarn")
  conf.set("yarn.resourcemanager.address", "dresman:8040")
  conf.set("fs.defaultFS", "hdfs://dhead:54310")
  conf.set("yarn.resourcemanager.scheduler.address", "dresman:8030")

  def runJob() = {
    JobUtils.setLibJars(conf)

    val args = new collection.mutable.ListBuffer[String]()

    args.append("--" + ConnectionParams.ZOOKEEPERS, "dzoo1")
    args.append("--" + ConnectionParams.ACCUMULO_INSTANCE, "dcloud")
    args.append("--" + ConnectionParams.ACCUMULO_USER, "root")
    args.append("--" + ConnectionParams.ACCUMULO_PASSWORD, "secret")
    //args.append("--" + ConnectionParams.CATALOG_TABLE, "AANNEX_SRI_ALL_VIS_RASTERS")

    val args2 = Args(args)

    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, args2)

    val job = new SampleRasterJobGrayScale(arguments)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }

}


object SampleRasterJobHistogram {

  def kvToMutation(k: Key, v: Value): Mutation = {
    val m = new Mutation(k.getRow)

    m.put(k.getColumnFamily, k.getColumnQualifier, k.getColumnVisibilityParsed, v)
    m
  }

  val conf = new Configuration
  conf.set("accumulo.monitor.address", "damaster"
  )
  conf.set("mapreduce.framework.name", "yarn")
  conf.set("yarn.resourcemanager.address", "dresman:8040")
  conf.set("fs.defaultFS", "hdfs://dhead:54310")
  conf.set("yarn.resourcemanager.scheduler.address", "dresman:8030")

  def runJob() = {
    JobUtils.setLibJars(conf)

    val args = new collection.mutable.ListBuffer[String]()

    args.append("--" + ConnectionParams.ZOOKEEPERS, "dzoo1")
    args.append("--" + ConnectionParams.ACCUMULO_INSTANCE, "dcloud")
    args.append("--" + ConnectionParams.ACCUMULO_USER, "root")
    args.append("--" + ConnectionParams.ACCUMULO_PASSWORD, "secret")
    //args.append("--" + ConnectionParams.CATALOG_TABLE, "AANNEX_SRI_ALL_VIS_RASTERS")

    val args2 = Args(args)

    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, args2)

    val job = new SampleRasterJobHistogram(arguments)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }

}