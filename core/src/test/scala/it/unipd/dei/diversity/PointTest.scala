package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.forAll

object PointTest extends Properties("Points") {

  property("Random point dimension") =
    forAll(Gen.choose[Int](2, 100)) { dim =>
      Point.random(dim).dimension == dim
    }

}
