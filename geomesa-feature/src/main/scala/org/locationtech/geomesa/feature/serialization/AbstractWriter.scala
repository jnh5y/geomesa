/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature.serialization

import java.util.{Collections => JCollections, List => JList, Map => JMap, UUID}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.feature.serialization.AbstractWriter.NULL_MARKER_STR
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._

/** [[DatumWriter]] definitions for writing (serializing) components of a [[org.opengis.feature.simple.SimpleFeature]].
  *
  */
trait AbstractWriter[Writer]
  extends PrimitiveWriter[Writer]
  with NullableWriter[Writer]
  with CollectionWriter[Writer]
  with GeometryWriter[Writer]
  with HintKeyWriter[Writer]
  with Logging {


  def writeUUID: DatumWriter[Writer, UUID] = (writer, uuid) => {
    writeLong(writer, uuid.getMostSignificantBits)
    writeLong(writer, uuid.getLeastSignificantBits)
  }

  /** A [[DatumWriter]] which writes the class name of ``obj`` and then the ``obj``.  If the object is ``null`` then
    * only a null marker will be written.
    *
    * @tparam T thpe of t
    */
  def writeGeneric[T]: DatumWriter[Writer, T] = (writer, obj) => {
    if (obj == null) {
      writeString(writer, NULL_MARKER_STR)
    } else {
      writeString(writer, obj.getClass.getName)
      selectWriter(obj.getClass.asInstanceOf[Class[T]])(writer, obj)
    }
  }

  /**
   * A [[DatumWriter]] for writing a map where the key and values may be any type.  The map may not be null. The writer
   * will call ``writeArrayStart(writer, map.size)`` and then, for each entry, call ``startItem`` followed by up to four
   * writes.  After writing all entries the reader will call ``endArray``.
   */
  def writeGenericMap: DatumWriter[Writer, JMap[AnyRef, AnyRef]] = (writer, map) => {

    // may not be able to write all entries - must pre-filter to know correct count
    import collection.JavaConverters.mapAsScalaMapConverter
    val filtered = map.asScala.filter {
      case (key, value) =>
        if (canSerialize(key)) {
          true
        } else {
          logger.warn(s"Can't serialize Map entry ($key,$value).  The map entry will be skipped.")
          false
        }
    }

    writeArrayStart(writer, filtered.size)

    filtered.foreach {
      case (key, value) =>
        startItem(writer)
        writeGeneric(writer, key)
        writeGeneric(writer, value)
    }

    endArray(writer)
  }

  def canSerialize(obj: AnyRef): Boolean = obj match {
    case key: Hints.Key => HintKeySerialization.canSerialize(key)
    case _ => true
  }

  /**
   * @param clazz the [[Class]] of the object to be written
   * @tparam T the type of the object to be written
   * @return a [[DatumWriter]] capable of writing object of the given ``clazz``
   */
  def selectWriter[T](clazz: Class[_ <: T], metadata: JMap[_ <: AnyRef, _ <: AnyRef] = JCollections.emptyMap(),
                      isNullable: isNullableFn = notNullable): DatumWriter[Writer, T] = {

    val writer = clazz match {
      case cls if classOf[java.lang.String].isAssignableFrom(cls) => writeString.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[java.lang.Integer].isAssignableFrom(cls) => writeInt.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[java.lang.Long].isAssignableFrom(cls) => writeLong.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[java.lang.Float].isAssignableFrom(cls) => writeFloat.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[java.lang.Double].isAssignableFrom(cls) => writeDouble.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[java.lang.Boolean].isAssignableFrom(cls) => writeBoolean.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[java.util.Date].isAssignableFrom(cls) => writeDate.asInstanceOf[DatumWriter[Writer, T]]

      case cls if classOf[UUID].isAssignableFrom(cls) => writeUUID.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[Geometry].isAssignableFrom(cls) => writeGeometry.asInstanceOf[DatumWriter[Writer, T]]
      case cls if classOf[Hints.Key].isAssignableFrom(cls) => writeHintKey.asInstanceOf[DatumWriter[Writer, T]]

      case c if classOf[JList[_]].isAssignableFrom(c) =>
        val elemClass = metadata.get(USER_DATA_LIST_TYPE).asInstanceOf[Class[_]]
        val elemWriter = selectWriter(elemClass, isNullable = isNullable)
        writeList(elemWriter).asInstanceOf[DatumWriter[Writer, T]]

      case c if classOf[JMap[_, _]].isAssignableFrom(c) =>
        val keyClass      = metadata.get(USER_DATA_MAP_KEY_TYPE).asInstanceOf[Class[_]]
        val valueClass    = metadata.get(USER_DATA_MAP_VALUE_TYPE).asInstanceOf[Class[_]]
        val keyWriter   = selectWriter(keyClass, isNullable = isNullable)
        val valueWriter = selectWriter(valueClass, isNullable = isNullable)
        writeMap(keyWriter, valueWriter).asInstanceOf[DatumWriter[Writer, T]]

      case _ => throw new IllegalArgumentException("Unsupported class: " + clazz)
    }

    if (isNullable(clazz)) {
      writeNullable(writer)
    } else {
      writer
    }
  }
}

object AbstractWriter {
  val NULL_MARKER_STR = "<null>"
}
