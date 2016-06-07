package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.{forAll, BooleanOperators, all}
import Utils._

object FarthestPointHeuristicTest extends Properties("FarthestPointHeuristic") {

  property("anticover") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.length >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val result = FarthestPointHeuristic.run(points, k, Distance.euclidean)
        val farness = minDistance(result, Distance.euclidean)
        val radius = maxMinDistance(result, points, Distance.euclidean)
        radius <= farness
      }
    }

  property("simpler implementation") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.size >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).toArray
        val actual = FarthestPointHeuristic.run(points, k, 0, Distance.euclidean _).toSet
        val expected = FarthestPointHeuristic.runSlow(points, k, Distance.euclidean).toSet
        s"$actual != $expected" |:(actual == expected)
      }
    }

}
