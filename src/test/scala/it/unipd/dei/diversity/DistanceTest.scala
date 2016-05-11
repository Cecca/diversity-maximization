package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import Distance.euclidean

object DistanceTest extends Properties("Distances") {

  def pointGen(dim: Int) =
    for(data <- Gen.listOfN(dim, Arbitrary.arbitrary[Double]))
      yield new Point(data.toArray)

  property("triangle inequality") =
    forAll(Gen.choose(2, 10)) { dim =>
      forAll(pointGen(dim), pointGen(dim), pointGen(dim)) { (a, b, c) =>
        euclidean(a, b) <= euclidean(a, c) + euclidean(c, b)
      }
    }

}
