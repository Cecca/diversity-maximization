package it.unipd.dei.diversity.wiki

class WikiBagOfWords(val title: String,
                     val categories: Set[String],
                     val words: Array[String],
                     val counts: Array[String]) {

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
    rawWords.view
      .filter(s => s.length > 1) // Filter out all single letter words
      .map(s => s.toLowerCase)   // Make everything lowercase


    ???
  }

}
