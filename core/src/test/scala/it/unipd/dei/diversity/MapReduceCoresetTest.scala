package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, forAll, all}

object MapReduceCoresetTest extends Properties("MapReduceCoreset") {

  def pointGen(dim: Int) =
    for(data <- Gen.listOfN(dim, Gen.choose[Double](0.0, 1.0)))
      yield new Point(data.toArray)

  property("number of points") =
    forAll(Gen.nonEmptyListOf(pointGen(1)), Gen.choose(2, 100), Gen.posNum[Int])
    { (points, kernelSize, numDelegates) =>
      (points.size > numDelegates) ==> {
        val coreset = MapReduceCoreset.run(
          points.toArray, kernelSize, numDelegates, Distance.euclidean)

        (coreset.length >= numDelegates) :| s"Coreset size is ${coreset.length}"
      }
    }

  property("no duplicates") =
    forAll(Gen.nonEmptyListOf(pointGen(1)), Gen.choose(2, 100), Gen.posNum[Int])
    { (points, kernelSize, numDelegates) =>
      (points.size > numDelegates) ==> {
        val coreset = MapReduceCoreset.run(
          points.toArray, kernelSize, numDelegates, Distance.euclidean)

        coreset.toSet.size == coreset.length
      }
    }

}
