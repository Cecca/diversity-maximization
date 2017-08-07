package it.unipd.dei.diversity.matroid

import org.apache.spark.ml.linalg.{Vector, Vectors}

object VectorUtils {

  val TWO_OVER_PI: Double = 2.0 / math.Pi
  val ONE_OVER_PI: Double = 1.0 / math.Pi

  def cosine(a: Vector, b: Vector): Double = {
    require(a.numNonzeros > 0, "First vector is zero-valued")
    require(b.numNonzeros > 0, "Second vector is zero-valued")
    require(a.size == b.size)
    var numerator: Double = 0.0
    a.foreachActive { case (i, ca) =>
      numerator += ca * b(i)
    }
    val denomA = Vectors.norm(a, 2)
    val denomB = Vectors.norm(b, 2)
    val res = numerator / (denomA * denomB)
    math.min(res, 1.0)
  }

}
