package it.unipd.dei.diversity

import org.scalacheck.Prop.{forAll, BooleanOperators}
import org.scalacheck.{Gen, Properties}

object MatchingHeuristicTest extends Properties("Matching") {

  val distance: (Point, Point) => Double = Distance.euclidean

  property("size") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.size >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val kSubset = MatchingHeuristic.run(points, k, distance).toSet
        (kSubset.size == k) :| s"Actual size ${kSubset.size}"
      }
  }

}
