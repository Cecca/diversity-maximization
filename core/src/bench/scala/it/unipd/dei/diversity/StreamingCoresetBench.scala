package it.unipd.dei.diversity

import org.scalameter.api._

/**
  * Benchmark StreamingCoreset: the baseline is summing
  * the euclidean norms of all the points in the stream.
  */
object StreamingCoresetBench extends Bench.OfflineReport {

  val spaceDimension = 128

  val distance: (Point, Point) => Double = Distance.euclidean

  val sets: Gen[Array[Point]] = for {
    size <- Gen.range("num-points")(10000, 30000, 10000)
  } yield Array.ofDim[Point](size).map{_ => Point.random(spaceDimension)}

  val ks: Gen[Int] = Gen.range("k")(10, 30, 10)

  val kernelSizes: Gen[Int] = Gen.range("k")(100, 300, 100)

  val params: Gen[(Array[Point], Int, Int)] = for {
    points <- sets
    size <- kernelSizes
    k <- ks
  } yield (points, size, k)

  performance of "streaming" in {

    measure method "baseline" in {
      using(params) in { case (points, size, k) =>
        val zero = Point(Array.fill[Double](spaceDimension)(0.0))
        var sum = 0.0
        var i = 0
        while (i < points.length) {
          sum += distance(points(i), zero)
          i += 1
        }
      }
    }

    measure method "coreset" in {
      using(params) in { case (points, size, k) =>
        val coreset = new StreamingCoreset[Point](size, k, distance)
        var i = 0
        while (i < points.length) {
          coreset.update(points(i))
          i += 1
        }
      }
    }


  }


}
