package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.{Distance, MapBagOfWords}
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

  def bagOfWordsPair =
    for {
      countsMap <- wordCounts
    } yield (filteredBagOfWords(countsMap), filteredBagOfWords(countsMap))

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

  def doubleEquality(a: Double, b: Double): Boolean = {
    val precision = 0.000000000000001
    math.abs(a-b) <= precision
  }

}
