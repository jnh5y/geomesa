import com.google.common.io.Resources
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.collection.JavaConversions._
import java.net.URL
import java.nio.charset.StandardCharsets
import org.locationtech.geomesa.trajectory._
import org.geotools.data.Query
import org.geotools.geometry.jts._

val path = "file:///opt/data/tdrive/2237.txt"
val data = Resources.readLines(new URL(path), StandardCharsets.UTF_8)

val converter = SimpleFeatureConverters.build[String]("tdrive", "tdrive")
val res = converter.processInput(data.iterator()).toList

val t = Trajectory(res.toArray)
val ts = new TrajectoryStore
ts.add(t)

val q = new Query
ts.getTrajectories(q)
val env = new ReferencedEnvelope(0, 1, 0, 1, null)
ts.getTrajectoriesIntersectingBBOX(env)

val env2 = new ReferencedEnvelope(116, 117, 39, 40, null)
ts.getTrajectoriesIntersectingBBOX(env2)
