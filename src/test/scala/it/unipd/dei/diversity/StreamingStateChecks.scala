package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.forAll
import StreamingState.minDistance

object StreamingStateChecks extends Properties("StreamingState") {

  property("farness") =
    forAll(Gen.choose(2, 100), Gen.posNum[Int], Gen.choose(2, 1000))
    { (kernelSize, numDelegates, n) =>
      val sState = new StreamingState(kernelSize, numDelegates, Distance.euclidean)
      (0 until n).foreach { _ =>
        sState.update(Point.random(1))
      }
      sState.threshold <= minDistance(sState.kernelPoints.toArray, Distance.euclidean)
    }

}
