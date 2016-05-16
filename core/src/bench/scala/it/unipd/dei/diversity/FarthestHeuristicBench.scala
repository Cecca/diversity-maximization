package it.unipd.dei.diversity

import org.scalameter.api._

object FarthestHeuristicBench extends Bench.OfflineReport {

  val distance: (Point, Point) => Double = Distance.euclidean

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("size")(100, 500, 100)
  } yield Array.ofDim[Point](size).map{_ => Point.random(10)}

  val ks: Gen[Int] = Gen.range("k")(10, 90, 10)

  val params: Gen[(Array[Point], Int)] = for {
    points <- sets
    k <- ks
  } yield (points, k)


  performance of "gmm" in {

    measure method "runSlow" in {
      using(params) in { case (points, k) =>
        FarthestPointHeuristic.runSlow(points, k, distance)
      }
    }

    measure method "run" in {
      using(params) in { case (points, k) =>
        FarthestPointHeuristic.run(points, k, distance)
      }
    }

  }

}
