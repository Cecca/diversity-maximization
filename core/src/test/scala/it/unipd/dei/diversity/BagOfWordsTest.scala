package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, forAll, all}

import scala.collection.mutable
import scala.util.Random

object BagOfWordsTest extends Properties("BagOfWords") {

  val vocabulary: Vector[Int] = (0 until 10000).toVector

  def randomTable(vocabulary: Vector[Int], size: Int) =
    mutable.HashMap(
      Random.shuffle(vocabulary).take(size).map { w =>
        (w, 1024*Random.nextDouble())
      } :_*)

  val wordCounts = for {
    size <- Gen.posNum[Int]
  } yield randomTable(vocabulary, size)

  val bows = for {
    wc1 <- wordCounts
    wc2 <- wordCounts
  } yield (
    new MapBagOfWords[Int](wc1), new MapBagOfWords[Int](wc2),
    new ArrayBagOfWords(wc1.toSeq), new ArrayBagOfWords(wc2.toSeq))

  property("words") =
    forAll(bows) { case (map1, map2, array1, array2) =>
      all (
        map1.words.toSet == array1.words.toSet,
        map2.words.toSet == array2.words.toSet
      )
    }

  property("distance") =
    forAll(bows) { case (map1, map2, array1, array2) =>
      val distMap = Distance.euclidean(map1, map2)
      val distArr = Distance.euclidean(array1, array2)
      doubleEquality(distMap, distArr) :| s"map: $distMap, arr: $distArr"
    }

  def doubleEquality(a: Double, b: Double, precision: Double = 0.0000000001): Boolean = {
    math.abs(a-b) <= precision
  }

}
