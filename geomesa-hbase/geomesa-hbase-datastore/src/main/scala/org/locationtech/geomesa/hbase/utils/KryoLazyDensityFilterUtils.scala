package org.locationtech.geomesa.hbase.utils

import java.util.Map.Entry

import com.vividsolutions.jts.geom.Envelope
//import org.apache.accumulo.core.data.{Key, Value}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by mahak on 3/29/17.
  */
object KryoLazyDensityFilterUtils {
  val DENSITY_SFT: SimpleFeatureType = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")
  val density_serializer: KryoFeatureSerializer  = new KryoFeatureSerializer(DENSITY_SFT, SerializationOptions.withoutId)
  /**
    * Encodes a sparse matrix into a byte array
    */
  def encodeResult(input: java.util.Map[(java.lang.Integer, java.lang.Integer), java.lang.Double]): Array[Byte] = {
    val result = input.asScala.toMap
    val output = KryoFeatureSerializer.getOutput(null)
    result.toList.groupBy(_._1._1).foreach { case (row, cols) =>
      output.writeInt(row, true)
      val x = cols.size
      output.writeInt(cols.size, true)
      cols.foreach { case (xy, weight) =>
        output.writeInt(xy._2, true)
        output.writeDouble(weight)
      }
    }
    output.toBytes
  }

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    println(s" Setting bytes to ${new String(bytes)}")
    //sf.setAttribute(0, bytes)
    sf.values(0) = bytes
//    sf.getAttribute(0).asInstanceOf[Array[Byte]]
    sf
  }


  type DensityResult = mutable.Map[(Int, Int), Double]
  type GridIterator = (SimpleFeature) => Iterator[(Double, Double, Double)]

  /**
    * Returns a mapping of simple features (returned from a density query) to weighted points in the
    * form of (x, y, weight)
    */
  def decodeResult(envelope: Envelope, gridWidth: Int, gridHeight: Int): GridIterator =
    decodeResult(new GridSnap(envelope, gridWidth, gridHeight))

  /**
    * Decodes a result feature into an iterator of (x, y, weight)
    */
  def decodeResult(gridSnap: GridSnap)(sf: SimpleFeature): Iterator[(Double, Double, Double)] = {
    val result = sf.getAttribute(0).asInstanceOf[Array[Byte]]
    val input = KryoFeatureSerializer.getInput(result, 0, result.length)
    new Iterator[(Double, Double, Double)]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext = input.position < input.limit
      override def next() = {
        if (colCount == 0) {
          x = gridSnap.x(input.readInt(true))
          colCount = input.readInt(true)
        }
        val y = gridSnap.y(input.readInt(true))
        val weight = input.readDouble()
        colCount -= 1
        (x, y, weight)
      }
    }
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
//  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
//    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
//    sf.setAttribute(1, GeometryUtils.zeroPoint)
//    (e: Entry[Key, Value]) => {
//      // set the value directly in the array, as we don't support byte arrays as properties
//      // TODO GEOMESA-823 support byte arrays natively
//      sf.values(0) = e.getValue.get()
//      sf
//    }
//  }


}
