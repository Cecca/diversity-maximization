package it.unipd.dei.diversity.words

import it.unipd.dei.diversity.ArrayBagOfWords

class UCIBagOfWords(val documentId: Int,
                    override val wordsArray: Array[Int],
                    override val countsArray: Array[Int])
extends ArrayBagOfWords(wordsArray, countsArray) with Serializable {

  def this(docId: Int, arrayPair: (Array[Int], Array[Int])) {
    this(docId, arrayPair._1, arrayPair._2)
  }

  def this(docId: Int, counts: Seq[(Int, Int)]) {
    this(docId, ArrayBagOfWords.buildArrayPair(counts))
  }

  override def equals(o: scala.Any): Boolean = o match {
    case other: UCIBagOfWords =>
      // FIXME: Make more efficient
      this.documentId == documentId &&
        this.wordsArray.toSeq == other.wordsArray.toSeq &&
        this.countsArray.toSeq == other.countsArray.toSeq
    case _ => false
  }

  override def hashCode(): Int = documentId

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
