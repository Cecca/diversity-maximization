package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.forAll
import scala.util.Random

object PointTest extends Properties("Points") {

  val randomGen = new Random()

  property("Random point dimension") =
    forAll(Gen.choose[Int](2, 100)) { dim =>
      Point.random(dim, randomGen).dimension == dim
    }

}
