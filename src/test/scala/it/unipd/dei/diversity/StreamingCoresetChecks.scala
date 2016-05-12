package it.unipd.dei.diversity

import org.scalacheck.Prop.forAll
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

}
