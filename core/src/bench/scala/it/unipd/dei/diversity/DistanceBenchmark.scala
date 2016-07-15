package it.unipd.dei.diversity

import org.scalameter.api._

import scala.collection.mutable
import scala.util.Random

object DistanceBenchmark extends Bench.OfflineReport {

  val vocabulary: Vector[Int] = (0 until 10000).toVector

  val sizes = Gen.exponential("size")(8, 8192, 2)
  val mapPairs = for {
    size <- sizes
  } yield (BOWBuilders.mapBOW(vocabulary, size), BOWBuilders.mapBOW(vocabulary, size))
  val arrayPairs = for {
    size <- sizes
  } yield (BOWBuilders.arrayBOW(vocabulary, size), BOWBuilders.arrayBOW(vocabulary, size))

  performance of "Distance.euclidean" in {

    measure method "MapBOW" in {
      using(mapPairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }
    measure method "ArrayBOW" in {
      using(arrayPairs) in { case (a, b) =>
        ArrayBagOfWords.euclidean(a, b)
      }
    }

  }

}

object BOWBuilders {

  def randomTable(vocabulary: Vector[Int], size: Int) =
    mutable.HashMap(
      Random.shuffle(vocabulary).take(size).map { w =>
        (w, Random.nextInt(1024).toDouble)
      } :_*)

  def mapBOW(vocabulary: Vector[Int], size: Int): MapBagOfWords[Int] =
    new MapBagOfWords[Int](randomTable(vocabulary, size))

  def arrayBOW(vocabulary: Vector[Int], size: Int): ArrayBagOfWords =
    ArrayBagOfWords(randomTable(vocabulary, size).toSeq)
}
