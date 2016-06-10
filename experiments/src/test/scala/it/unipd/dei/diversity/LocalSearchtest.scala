package it.unipd.dei.diversity

import org.scalacheck.Prop.{forAll,BooleanOperators}
import org.scalacheck.{Gen, Properties}

object LocalSearchtest extends Properties("LocalSearch") {

  val distance: (Point, Point) => Double = Distance.euclidean
  val diversity: (IndexedSeq[Point], (Point, Point) => Double) => Double =
    Diversity.clique[Point]

  property("size") =
    forAll(Gen.listOf[Double](Gen.choose[Double](0.0, 1.0)), Gen.choose[Int](2, 100))
    { (pts: List[Double], k: Int) =>
      (pts.size >= 2 && k < pts.size) ==> {
        val points = pts.map(p => Point(p)).distinct.sorted.toArray
        val expected = LocalSearch.runSlow(points, k, 1.0, distance, diversity).toSet
        val actual = LocalSearch.run(points, k, 1.0, distance, diversity).toSet
        (expected == actual) :|
          s"""
             |expected (div=${diversity(expected.toArray[Point], distance)}):
             |  ${expected.toSeq.sorted}
             |actual   (div=${diversity(actual.toArray[Point], distance)}):
             |  ${actual.toSeq.sorted}
             |""".stripMargin
      }
    }

}
