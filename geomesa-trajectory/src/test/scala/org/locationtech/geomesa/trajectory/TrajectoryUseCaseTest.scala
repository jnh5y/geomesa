/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.trajectory

import java.nio.charset.StandardCharsets

import com.google.common.io.Resources
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TrajectoryUseCaseTest extends Specification {

  // Read the data
  import scala.collection.JavaConversions._
  val data = Resources.readLines(Resources.getResource("2237.txt"), StandardCharsets.UTF_8)

  "We must read input data " >> {

    val converter = SimpleFeatureConverters.build[String]("tdrive", "tdrive")
    converter must not beNull
    val res = converter.processInput(data.iterator()).toList
    converter.close()

    true mustEqual true

  }

}
