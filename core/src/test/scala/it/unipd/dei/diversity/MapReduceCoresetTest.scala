package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, forAll, all}

object MapReduceCoresetTest extends Properties("MapReduceCoreset") {

  def pointGen(dim: Int) =
    for(data <- Gen.listOfN(dim, Gen.choose[Double](0.0, 1.0)))
      yield new Point(data.toArray)

  property("number of points") =
    forAll(Gen.nonEmptyListOf(pointGen(1))) { points =>
      forAll(Gen.choose(2, points.length / 2)) { k =>
        forAll(Gen.choose(k, points.length / 2)) { kernelSize =>
          (points.size > k) ==> {
            val coreset = MapReduceCoreset.run(
              points.toArray, kernelSize, k, Distance.euclidean)

            (coreset.length >= k) :| s"Coreset size is ${coreset.length}"
          }
        }
      }
    }

  property("no duplicates") =
    forAll(Gen.nonEmptyListOf(pointGen(1))) { points =>
      forAll(Gen.choose(2, points.length / 2)) { k =>
        forAll(Gen.choose(k, points.length / 2)) { kernelSize =>
          (points.size > k) ==> {
            val coreset = MapReduceCoreset.run(
              points.toArray, kernelSize, k, Distance.euclidean)

            coreset.points.toSet.size == coreset.length
          }
        }
      }
    }

  val anticoverParameters = for {
    points <- Gen.nonEmptyListOf(pointGen(1))
    k <- Gen.choose(2, points.length)
    kernelSize <- Gen.choose(k, points.length)
  } yield (points, k, kernelSize)

  property("anticover") =
    forAll(Gen.nonEmptyListOf(pointGen(1))) { points =>
      forAll(Gen.choose(2, points.length/2)) { k =>
        forAll(Gen.choose(k, points.length/2)) { kernelSize =>
          val coreset = MapReduceCoreset.run(
            points.toArray, kernelSize, k, Distance.euclidean)

          val radius =
            Utils.maxMinDistance(coreset.delegates, coreset.kernel, Distance.euclidean)

          val farness =
            Utils.minDistance(coreset.kernel, Distance.euclidean)

          (radius <= farness) :| s"radius $radius, farness $farness"
        }
      }
    }

}
