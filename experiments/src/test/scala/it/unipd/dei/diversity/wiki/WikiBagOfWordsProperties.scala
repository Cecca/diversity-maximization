package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.Distance
import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck.Gen

import scala.collection.mutable
import scala.util.Random

object WikiBagOfWordsProperties extends Properties("WikiBagOfWords") {

  def wordWithCount: Gen[(String, Int)] =
    for {
      chars <- Gen.nonEmptyListOf(Gen.alphaChar)
      count <- Gen.choose(1, 100)
    } yield (chars.mkString(""), count)

  def wordCounts = Gen.nonEmptyMap(wordWithCount)

  def filteredBagOfWords(wordCounts: Map[String, Int]) = {
    val (words, counts) = wordCounts.filter(_ => Random.nextBoolean()).toSeq.sortBy(_._1).unzip
    if (words.isEmpty) {
      val (word, count) = wordCounts.head
      new WikiBagOfWords("", Set.empty, Array(word), Array(count))
    } else {
      new WikiBagOfWords("", Set.empty, words.toArray, counts.toArray)
    }
  }

  def buildBagOfWords(wordCounts: Map[String, Int]) = {
    val (words, counts) = wordCounts.toSeq.sortBy(_._1).unzip
    new WikiBagOfWords("", Set.empty, words.toArray, counts.toArray)
  }

  def bagOfWords =
    for {
      countsMap <- wordCounts
    } yield buildBagOfWords(countsMap)

  def orthogonalBagOfWords = wordCounts.flatMap { countsMapA =>
    wordCounts.flatMap { countsMapB =>
      val intersection = countsMapA.keySet.intersect(countsMapB.keySet)
      val wordsA = countsMapA -- intersection
      val wordsB = countsMapB -- intersection
      (buildBagOfWords(wordsA), buildBagOfWords(wordsB))
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

  property("cosine similarity conformance with generic implementation") =
    forAll(bagOfWordsPair) { case (a, b) =>
      val expected = Distance.cosineSimilarity(a, b)
      val actual = WikiBagOfWords.cosineSimilarity(a, b)
      (expected == actual) :|
        s"""
           | "A" words: ${a.wordsArray.zip(a.countsArray).toSeq}
           | "B" words: ${b.wordsArray.zip(b.countsArray).toSeq}
           | expected != actual
           | $expected != $actual
         """.stripMargin
    }

  property("cosine similarity of the same page") =
    forAll(bagOfWords) { bow =>
      val sim = WikiBagOfWords.cosineSimilarity(bow, bow)
      doubleEquality(sim, 1.0) :| s"Similarity is $sim instead of 1.0"
    }

  property("cosine distance of same bag of words") =
    forAll(bagOfWords) { bow =>
      val dist = WikiBagOfWords.cosineDistance(bow, bow)
      // here we use a lower precision in the equality comparison because of
      // the propagation of errors in floating point operations
      doubleEquality(dist, 0.0, 0.0000001) :| s"Distance is $dist instead of 0.0"
    }

  property("cosine distance of unrelated bag of words") =
    forAll(orthogonalBagOfWords) { case (a, b) =>
      val dist = WikiBagOfWords.cosineDistance(a, b)
      doubleEquality(dist, 1.0) :| s"Distance is $dist instead of 1.0"
    }

  property("cosine distance triangle inequality") =
    forAll(bagOfWordsTriplet) { case (a, b, c) =>
      val dFn: (WikiBagOfWords, WikiBagOfWords) => Double = WikiBagOfWords.cosineDistance
      dFn(a, b) + dFn(b, c) >= dFn(a, c)
    }

  def doubleEquality(a: Double, b: Double, precision: Double = 0.000000000001): Boolean = {
    math.abs(a-b) <= precision
  }

}
