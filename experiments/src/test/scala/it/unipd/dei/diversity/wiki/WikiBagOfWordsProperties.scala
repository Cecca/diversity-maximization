package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.Distance
import org.scalacheck._
import org.scalacheck.Prop.{BooleanOperators, forAll, all}
import org.scalacheck.Gen

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

}
