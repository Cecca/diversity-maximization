package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.PerformanceMetrics
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by ceccarel on 06/07/17.
  */
object MlLibDistances {

  private val TWO_OVER_PI: Double = 2.0 / math.Pi
  private val ONE_OVER_PI: Double = 1.0 / math.Pi

  private def cosine(a: Vector, b: Vector): Double = {
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

  def cosineDistanceFull(a: Vector, b: Vector): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    val cos = cosine(a, b)
    require(-1.0 <= cos && cos <= 1.0, s"Cosine out of range: $cos")
    val dist = ONE_OVER_PI * math.acos(cos)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity, "Points at infinite distance!!")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

  def cosineDistanceHalf(a: Vector, b: Vector): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    val cos = cosine(a, b)
    require(-1.0 <= cos && cos <= 1.0, s"Cosine out of range: $cos")
    val dist = TWO_OVER_PI * math.acos(cos)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity, "Points at infinite distance!!")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

}
