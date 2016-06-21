package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.{BagOfWords, Distance, Diversity, PorterStemmer}

import scala.io.Source

class WikiBagOfWords(val title: String,
                     val categories: Set[String],
                     val wordsArray: Array[String],
                     val countsArray: Array[Int])
extends BagOfWords[String] with Serializable {

  // Words array MUST be sorted, otherwise the specialized
  // implementation of the euclidean distance breaks
  require(wordsArray.sorted.sameElements(wordsArray))

  override def words: Iterator[String] = wordsArray.iterator

  override def apply(word: String): Int = {
    val idx = wordsArray.indexOf(word)
    if (idx >= 0) {
      countsArray(idx)
    } else {
      0
    }
  }

  override def toString: String =
    s"$title: ($categories)\n${wordsArray.zip(countsArray).mkString(", ")}"

}

object WikiBagOfWords {

  def fromLine(line: String): WikiBagOfWords = {
    val tokens = line.split("\t")
    require(tokens.length == 3 || tokens.length == 4)
    val title = tokens(0)
    val categories = tokens(1)
      .replaceAll("wordnet_", "")
      .replaceAll("wikicat_", "")
      .replace("<", "")
      .replace(">", "")
      .split(",")
      .toSet
    val rawWords =
      if (tokens.length == 3) {
        tokens(2).split(" ")
      } else {
        tokens(3).split(" ")
      }
    val wordCounts = rawWords.view
      .filter(s => s.length > 1)        // Filter out all single letter words
      .map(s => s.toLowerCase)          // Make everything lowercase
      .map(s => PorterStemmer.stem(s))  // stem the words
      .groupBy(identity)
      .map({case (w, occurences) => (w, occurences.size)})
      .toSeq
      .sortBy(_._1)

    val (words, counts) = wordCounts.unzip

    new WikiBagOfWords(title, categories, words.toArray, counts.toArray)
  }

  def cosineSimilarity(a: WikiBagOfWords, b: WikiBagOfWords): Double = {
    var numerator = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        numerator += a.countsArray(aIdx) * b.countsArray(bIdx)
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        aIdx += 1
      } else {
        bIdx += 1
      }
    }

    var denominatorA = 0.0
    aIdx = 0
    while (aIdx < a.countsArray.length) {
      val v = a.countsArray(aIdx)
      denominatorA += v*v
      aIdx += 1
    }
    var denominatorB = 0.0
    bIdx = 0
    while (bIdx < b.countsArray.length) {
      val v = b.countsArray(bIdx)
      denominatorB += v*v
      bIdx += 1
    }

    val res = numerator / ( math.sqrt(denominatorA) * math.sqrt(denominatorB) )
    // See the comment in Distance.cosineSimilarity for the motivation of the
    // following operation.
    math.min(1.0, res)
  }

  def cosineDistance(a: WikiBagOfWords, b: WikiBagOfWords): Double = {
    2*math.acos(cosineSimilarity(a,b)) / math.Pi
  }

  def euclidean(a: WikiBagOfWords, b: WikiBagOfWords): Double = {
    var sum = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        val diff = a.countsArray(aIdx) - b.countsArray(bIdx)
        sum += diff * diff
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        val diff = a.countsArray(aIdx)
        sum += diff*diff
        aIdx += 1
      } else {
        val diff = b.countsArray(bIdx)
        sum += diff * diff
        bIdx += 1
      }
    }

    while (aIdx < a.wordsArray.length) {
      val diff = a.countsArray(aIdx)
      sum += diff*diff
      aIdx += 1
    }
    while (bIdx < b.wordsArray.length) {
      val diff = b.countsArray(bIdx)
      sum += diff*diff
      bIdx += 1
    }

    val res = math.sqrt(sum)
    assert(res < Double.PositiveInfinity, "The distance cannot be infinite! Check your inputs.")
    res
  }

  def main(args: Array[String]) {
    val text =
      if (args.length > 0) Source.fromFile(args(0))
      else Source.fromInputStream(System.in)
    val bows = text.getLines().map(s => WikiBagOfWords.fromLine(s)).toVector
    text.close()
    println(Diversity.clique(bows, Distance.euclidean[String]))
  }

}
