package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.{BagOfWords, Distance, Diversity, PorterStemmer}

import scala.io.Source

class WikiBagOfWords(val title: String,
                     val categories: Set[String],
                     val wordsArray: Array[String],
                     val countsArray: Array[Int])
extends BagOfWords[String] with Serializable {

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

  def main(args: Array[String]) {
    val text =
      if (args.length > 0) Source.fromFile(args(0))
      else Source.fromInputStream(System.in)
    val bows = text.getLines().map(s => WikiBagOfWords.fromLine(s)).toVector
    text.close()
    println(Diversity.clique(bows, Distance.euclidean[String]))
  }

}
