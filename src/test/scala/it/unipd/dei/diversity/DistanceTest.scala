package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import Distance.euclidean

object DistanceTest extends Properties("Distances") {

  def pointGen(dim: Int) =
    for(data <- Gen.listOfN(dim, Arbitrary.arbitrary[Double]))
      yield new Point(data.toArray)

  val pointTriplet = for {
    dim <- Gen.choose(2, 10)
    a   <- pointGen(dim)
    b   <- pointGen(dim)
    c   <- pointGen(dim)
  } yield (a, b, c)

  property("triangle inequality") =
    forAll(pointTriplet) { case (a, b, c) =>
      euclidean(a, b) <= euclidean(a, c) + euclidean(c, b)
    }

}
