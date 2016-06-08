package it.unipd.dei.diversity.words

import it.unipd.dei.diversity.BagOfWords

import scala.collection.immutable.HashSet

class UCIBagOfWords(val documentId: Int,
                    override val wordCounts: Map[Int, Int])
extends BagOfWords[Int] with Serializable {

  override val words: Set[Int] =
    HashSet[Int](wordCounts.keys.toSeq :_*)

  override def equals(o: scala.Any): Boolean = o match {
    case other: UCIBagOfWords =>
      this.documentId == documentId && this.wordCounts == other.wordCounts
    case _ => false
  }

  override def hashCode(): Int = 31*documentId + wordCounts.hashCode()

  override def toString: String =
    s"Document: $documentId\n$wordCounts"

  def toString(wordMap: Map[Int, String]): String = {
    val wrapCol = 80
    val sb = new StringBuilder()
    sb ++= s"Document $documentId\n"
    var columnCnt = 0
    for ((wordId, cnt) <- wordCounts) {
      if (columnCnt >= wrapCol) {
        sb ++= "\n"
        columnCnt = 0
      }
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
