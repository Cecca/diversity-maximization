package it.unipd.dei.diversity

import org.scalameter.api._

object LocalSearchBench extends Bench.OfflineReport {

  val distance: (Point, Point) => Double = Distance.euclidean

  val diversity: (IndexedSeq[Point], (Point, Point) => Double) => Double =
    Diversity.clique[Point]

  val epsilon = 1.0

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("size")(100, 300, 100)
  } yield Array.ofDim[Point](size).map{_ => Point.random(10)}

  val ks: Gen[Int] = Gen.range("k")(10, 20, 10)

  val params: Gen[(Array[Point], Int)] = for {
    points <- sets
    k <- ks
  } yield (points, k)

  performance of "local search" in {

    measure method "local search" in {
      using(params) in { case (points, k) =>
        LocalSearch.run(points, k, epsilon, distance, diversity)
      }
    }
    
    measure method "matching heuristic" in {
      using(params) in { case (points, k) =>
        MatchingHeuristic.run(points, k, distance)
      }
    }
  }

}