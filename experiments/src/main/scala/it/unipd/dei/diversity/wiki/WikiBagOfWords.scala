package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.{BagOfWords, Distance, Diversity, PorterStemmer}

import scala.io.Source

class WikiBagOfWords(val title: String,
                     val categories: Set[String],
                     val wordsArray: Array[String],
                     val scoresArray: Array[Double])
extends BagOfWords[String] with Serializable {

  // A bag of words without words can't be built
  require(wordsArray.length > 0, s"A bag of words must have at least a word (page $title)")

  // Words array MUST be sorted, otherwise the specialized
  // implementation of the euclidean distance breaks
  require(wordsArray.sorted.sameElements(wordsArray))

  // Moreover, we want all the scores to be positive
  require(scoresArray.map(_ >= 0.0).reduce(_ && _), "All the scores must be positive")

  override def words: Iterator[String] = wordsArray.iterator

  override def apply(word: String): Double = {
    val idx = wordsArray.indexOf(word)
    if (idx >= 0) {
      scoresArray(idx)
    } else {
      0
    }
  }

  override def toString: String =
    s"$title: ($categories)\n${wordsArray.zip(scoresArray).mkString(", ")}"

}

object WikiBagOfWords {

  /**
    * Parses the given line into a [[WikiBagOfWords]]. The line _must_
    * have 3 or 4 tab-separated fields, with the following meaning
    *
    *  1. The title of the page
    *  2. The categories of the page
    *  3. The ranking of the page. This is ignored.
    *  4. The words of the page
    *
    * If there are only 3 fields, then the page ranking is assumed to be
    * missing. Since it is irrelevant to our purposes, it's fine.
    *
    * @param line a tab-separated string
    * @return maybe a [[WikiBagOfWords]]
    */
  def fromLine(line: String): Option[WikiBagOfWords] = {
    val tokens = line.split("\t")
    require(tokens.length == 3 || tokens.length == 4)
    // Check if the last token is a number. This is needed because
    // the wiki dataset has the page rank score in each page, but
    // some pages have no text. This leads to the page rank being
    // interpreted as the text, which is obviously incorrect. Since
    // pages with no text cannot be turned to valid bag of words, the
    // safest course is to filter them out.
    if (tokens.length == 3 && tokens(2).matches("""[+-]?\d+(\.\d+)?""")){
      return None
    }
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
      .map({case (w, occurences) => (w, occurences.size.toDouble)})
      .toSeq
      .sortBy(_._1)

    if (wordCounts.isEmpty) {
      None
    } else {
      val (words, counts) = wordCounts.unzip
      Some(new WikiBagOfWords(title, categories, words.toArray, counts.toArray))
    }
  }

  def cosineSimilarity(a: WikiBagOfWords, b: WikiBagOfWords): Double = {
    var numerator = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        numerator += a.scoresArray(aIdx) * b.scoresArray(bIdx)
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
    while (aIdx < a.scoresArray.length) {
      val v = a.scoresArray(aIdx)
      denominatorA += v*v
      aIdx += 1
    }
    var denominatorB = 0.0
    bIdx = 0
    while (bIdx < b.scoresArray.length) {
      val v = b.scoresArray(bIdx)
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

  def jaccard(a: WikiBagOfWords, b: WikiBagOfWords): Double = {
    var numerator   = 0.0
    var denominator = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        numerator   += math.min(a.scoresArray(aIdx), b.scoresArray(bIdx))
        denominator += math.max(a.scoresArray(aIdx), b.scoresArray(bIdx))
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        denominator += a.scoresArray(aIdx)
        aIdx += 1
      } else {
        denominator += b.scoresArray(bIdx)
        bIdx += 1
      }
    }
    while (aIdx < a.wordsArray.length) {
      denominator += a.scoresArray(aIdx)
      aIdx += 1
    }
    while (bIdx < b.wordsArray.length) {
      denominator += b.scoresArray(bIdx)
      bIdx += 1
    }
    require(aIdx == a.wordsArray.length, s"$aIdx < ${a.wordsArray.length}")
    require(bIdx == b.wordsArray.length, s"$bIdx < ${b.wordsArray.length}")

    1.0 - (numerator/denominator)
  }

  def euclidean(a: WikiBagOfWords, b: WikiBagOfWords): Double = {
    var sum = 0.0
    var aIdx = 0
    var bIdx = 0
    while(aIdx < a.wordsArray.length && bIdx < b.wordsArray.length) {
      if (a.wordsArray(aIdx) == b.wordsArray(bIdx)) {
        val diff = a.scoresArray(aIdx) - b.scoresArray(bIdx)
        sum += diff * diff
        aIdx += 1
        bIdx += 1
      } else if (a.wordsArray(aIdx) < b.wordsArray(bIdx)) {
        val diff = a.scoresArray(aIdx)
        sum += diff*diff
        aIdx += 1
      } else {
        val diff = b.scoresArray(bIdx)
        sum += diff * diff
        bIdx += 1
      }
    }

    while (aIdx < a.wordsArray.length) {
      val diff = a.scoresArray(aIdx)
      sum += diff*diff
      aIdx += 1
    }
    while (bIdx < b.wordsArray.length) {
      val diff = b.scoresArray(bIdx)
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
    println(Diversity.clique(bows.map(_.get), Distance.euclidean[String]))
  }

}
