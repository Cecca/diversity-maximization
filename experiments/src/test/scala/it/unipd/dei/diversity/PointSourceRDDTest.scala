package it.unipd.dei.diversity

import it.unipd.dei.diversity.source.{PointSource, PointSourceRDD}
import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck.{Gen, Properties}

class PointSourceRDDTest extends Properties("PointSourceRDD") {

  property("number of points") =
    forAll(Gen.choose(16, 512), Gen.choose(2, 64)) { (size, k) =>
      LocalSparkContext.withSpark { sc =>
        val source = PointSource("random-gaussian-sphere", 1, size, k, Distance.euclidean)
        val input = new PointSourceRDD(sc, source, sc.defaultParallelism)

        val numPoints = input.count()
        (numPoints >= size) :| s"Number of points $numPoints, size $size"
      }
    }

}
