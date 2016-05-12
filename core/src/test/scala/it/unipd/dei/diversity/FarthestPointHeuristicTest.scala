package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.{forAll, BooleanOperators}
import Utils._

object FarthestPointHeuristicTest extends Properties("FarthestPointHeuristic") {

  property("anticover") = forAll { (pts: Array[Double], k: Int) =>
    (pts.length >= 2 && k >= 2) ==> {
      val points = pts.map(p => Point(p))
      val result = FarthestPointHeuristic.run(points, k, Distance.euclidean)
      val farness = minDistance(result, Distance.euclidean)
      val radius = maxMinDistance(result, points, Distance.euclidean)
      radius <= farness
    }

  }

}
