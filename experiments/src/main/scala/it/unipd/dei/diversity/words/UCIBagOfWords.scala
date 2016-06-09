package it.unipd.dei.diversity.words

import java.util

import it.unipd.dei.diversity.ArrayBagOfWords

class UCIBagOfWords(val documentId: Int,
                    wordCounts: Seq[(Int, Int)])
extends ArrayBagOfWords(wordCounts) with Serializable {

  override def equals(o: scala.Any): Boolean = o match {
    case other: UCIBagOfWords =>
      // FIXME: Make more efficient
      this.documentId == documentId &&
        this.wordsArray.toSeq == other.wordsArray.toSeq &&
        this.countsArray.toSeq == other.countsArray.toSeq
    case _ => false
  }

  override def hashCode(): Int =
    31*documentId + 7*util.Arrays.hashCode(wordsArray) + util.Arrays.hashCode(countsArray)

  override def toString: String =
    s"Document $documentId"

  def toString(wordMap: Map[Int, String]): String = {
    val wrapCol = 80
    val sb = new StringBuilder()
    sb ++= s"Document $documentId\n"
    var columnCnt = 0
    for (wordId <- words) {
      if (columnCnt >= wrapCol) {
        sb ++= "\n"
        columnCnt = 0
      }
      val cnt = this(wordId)
      val str =
        if (wordMap.contains(wordId)) {
          s"${wordMap(wordId)}($cnt) "
        } else {
          s"!$wordId!($cnt) "
        }
      columnCnt += str.length
      sb ++= str
    }
    sb.toString()
  }

}
