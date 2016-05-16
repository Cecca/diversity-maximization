package it.unipd.dei.diversity

import org.scalameter.api._

object MatchingHeuristicBench extends Bench.OfflineReport {

  val distance: (Point, Point) => Double = Distance.euclidean

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("size")(100, 500, 100)
  } yield Array.ofDim[Point](size).map{_ => Point.random(10)}

  val ks: Gen[Int] = Gen.range("k")(10, 90, 10)

  val params: Gen[(Array[Point], Int)] = for {
    points <- sets
    k <- ks
  } yield (points, k)

  performance of "matching" in {
    measure method "baseline" in {
      using(params) in { case (points, k) =>
        var sum = 0.0
        var i = 0
        while (i<points.length) {
          var j = i +1
          while (j < points.length) {
            sum += distance(points(i), points(j))
            j += 1
          }
          i += 1
        }
      }
    }

    measure method "matching heuristic" in {
      using(params) in { case (points, k) =>
        MatchingHeuristic.run(points, k, distance)
      }
    }
  }

}
