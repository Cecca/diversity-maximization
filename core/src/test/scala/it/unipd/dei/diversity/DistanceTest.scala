package it.unipd.dei.diversity

import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, forAll, all}
import org.scalacheck.Gen
import Distance.euclidean

import scala.collection.mutable
import scala.util.Random

object DistanceTest extends Properties("Distances") {

  def pointGen(dim: Int) =
    for (data <- Gen.listOfN(dim, Gen.choose[Double](0.0, Long.MaxValue.toDouble)))
      yield new Point(data.toArray)

  def wordWithCount: Gen[(String, Int)] =
    for {
      chars <- Gen.nonEmptyListOf(Gen.alphaChar)
      count <- Gen.choose(1, 100)
    } yield (chars.mkString(""), count)

  def wordCounts = Gen.nonEmptyMap(wordWithCount)

  def bagOfWords =
    for {
      countsMap <- wordCounts
    } yield new MapBagOfWords[String](mutable.HashMap(countsMap.toSeq :_*))

  def filteredBagOfWords(wordCounts: Map[String, Int]) = {
    val words = wordCounts.filter(_ => Random.nextBoolean()).toSeq
    if (words.isEmpty) {
      new MapBagOfWords[String](mutable.HashMap(wordCounts.head))
    } else {
      new MapBagOfWords[String](mutable.HashMap(words: _*))
    }
  }

  def bagOfWordsPair =
    for {
      countsMap <- wordCounts
    } yield (filteredBagOfWords(countsMap), filteredBagOfWords(countsMap))

  property("euclidean triangle inequality") =
    forAll(Gen.choose(2, 10)) { dim =>
      forAll(pointGen(dim), pointGen(dim), pointGen(dim)) { (a, b, c) =>
        euclidean(a, b) <= euclidean(a, c) + euclidean(c, b)
      }
    }

  property("cosine similarity range") =
    forAll(bagOfWordsPair) { case (a, b) =>
      val sim = Distance.cosineSimilarity(a, b)
      all (
        s"Similarity is $sim and should be >= 0.0" |: sim >= 0,
        s"Similarity is $sim and should be <= 1.0" |: sim <= 1
      )
    }

  property("cosine similarity of same bag of words") =
    forAll(bagOfWords) { bow =>
      val sim = Distance.cosineSimilarity(bow, bow)
      doubleEquality(sim, 1.0) :| s"Similarity is $sim instead of 1.0"
    }

  def doubleEquality(a: Double, b: Double): Boolean = {
    val precision = 0.000000000000001
    math.abs(a-b) <= precision
  }
}
