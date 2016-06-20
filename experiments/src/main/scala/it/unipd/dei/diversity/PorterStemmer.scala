package it.unipd.dei.diversity

import org.tartarus.Stemmer

/**
  * Thin wrapper around [[org.tartarus.Stemmer]]
  */
object PorterStemmer {

  def stem(word: String): String = {
    val stemmer = new Stemmer()
    stemmer.add(word.toCharArray, word.length)
    stemmer.stem()
    stemmer.toString
  }

}
