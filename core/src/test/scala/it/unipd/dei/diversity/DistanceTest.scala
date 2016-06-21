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

  def orthogonalBagOfWords = wordCounts.flatMap { countsMapA =>
    wordCounts.flatMap { countsMapB =>
      val intersection = countsMapA.keySet.intersect(countsMapB.keySet)
      val wordsA = countsMapA -- intersection
      val wordsB = countsMapB -- intersection
      (new MapBagOfWords[String](mutable.HashMap(wordsA.toSeq: _*)),
        new MapBagOfWords[String](mutable.HashMap(wordsB.toSeq: _*)))
    }
  }

  def bagOfWordsPair =
    for {
      countsMap <- wordCounts
    } yield (filteredBagOfWords(countsMap), filteredBagOfWords(countsMap))

  def bagOfWordsTriplet =
    for {
      countsMap1 <- wordCounts
      countsMap2 <- wordCounts
      countsMap3 <- wordCounts
    } yield (
      filteredBagOfWords(countsMap1 ++ countsMap2 ++ countsMap3),
      filteredBagOfWords(countsMap1 ++ countsMap2 ++ countsMap3),
      filteredBagOfWords(countsMap1 ++ countsMap2 ++ countsMap3)
      )

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

  property("cosine similarity of unrelated bag of words") =
    forAll(orthogonalBagOfWords) { case (a, b) =>
      val sim = Distance.cosineSimilarity(a, b)
      doubleEquality(sim, 0.0) :| s"Similarity is $sim instead of 0.0"
    }

  property("cosine distance of same bag of words") =
    forAll(bagOfWords) { bow =>
      val dist = Distance.cosineDistance(bow, bow)
      // here we use a lower precision in the equality comparison because of
      // the propagation of errors in floating point operations
      doubleEquality(dist, 0.0, 0.0000001) :| s"Distance is $dist instead of 0.0"
    }

  property("cosine distance of unrelated bag of words") =
    forAll(orthogonalBagOfWords) { case (a, b) =>
      val dist = Distance.cosineDistance(a, b)
      doubleEquality(dist, 1.0) :| s"Distance is $dist instead of 1.0"
    }

  property("cosine distance triangle inequality") =
    forAll(bagOfWordsTriplet) { case (a, b, c) =>
      val dFn: (BagOfWords[String], BagOfWords[String]) => Double = Distance.cosineDistance[String]
      dFn(a, b) + dFn(b, c) >= dFn(a, c)
    }

  def doubleEquality(a: Double, b: Double, precision: Double = 0.000000000001): Boolean = {
    math.abs(a-b) <= precision
  }
}
