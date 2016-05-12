package it.unipd.dei.diversity

import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck._

object StreamingCoresetChecks extends Properties("StreamingCoreset") {

  property("farness") =
    forAll(Gen.choose(2, 100), Gen.posNum[Int], Gen.choose(2, 1000))
    { (kernelSize, numDelegates, n) =>
      val coreset = new StreamingCoreset(kernelSize, numDelegates, Distance.euclidean)
      (0 until n).foreach { _ =>
        coreset.update(Point.random(1))
      }

      coreset.numKernelPoints == 1 || coreset.threshold <= coreset.minKernelDistance
    }

  property("radius") =
    forAll(Gen.choose(2, 100), Gen.choose(1, 100), Gen.choose(2, 1000))
    { (kernelSize, numDelegates, n) =>
      (n > kernelSize) ==> {
        val coreset = new StreamingCoreset(kernelSize, numDelegates, Distance.euclidean)
        (0 until n).foreach { _ =>
          coreset.update(Point.random(1))
        }

        (coreset.delegatesRadius <= 2 * coreset.threshold) :|
          s"Radius: ${coreset.delegatesRadius} > 2*threshold: ${coreset.threshold}"
      }
    }


}
