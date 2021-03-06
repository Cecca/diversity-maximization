// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity.words

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import it.unipd.dei.diversity.ArrayBagOfWords

class DocumentBagOfWords(val documentId: String,
                         override val wordsArray: Array[Int],
                         override val scoresArray: Array[Double])
extends ArrayBagOfWords(wordsArray, scoresArray) with Serializable {

  def this(docId: String, arrayPair: (Array[Int], Array[Double])) {
    this(docId, arrayPair._1, arrayPair._2)
  }

  def this(docId: String, counts: Seq[(Int, Double)]) {
    this(docId, ArrayBagOfWords.buildArrayPair(counts))
  }

  def numWords: Int = wordsArray.length

  override def equals(o: scala.Any): Boolean = o match {
    case other: DocumentBagOfWords =>
      // FIXME: Make more efficient
      this.documentId == documentId &&
        this.wordsArray.toSeq == other.wordsArray.toSeq &&
        this.scoresArray.toSeq == other.scoresArray.toSeq
    case _ => false
  }

  override def hashCode(): Int = documentId.hashCode()

  override def toString: String =
    s"Document $documentId"

  def toLongString: String =
    wordsArray.zip(scoresArray).sorted
      .map({wc => s"${wc._1} -> ${wc._2}"}).mkString("[", ", ", "]")

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

class BOWKryoSerializer extends Serializer[DocumentBagOfWords] {
  override def write(kryo: Kryo, output: Output, bow: DocumentBagOfWords): Unit = {
    kryo.writeObject(output, bow.documentId)
    kryo.writeObject(output, bow.wordsArray)
    kryo.writeObject(output, bow.scoresArray)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[DocumentBagOfWords]): DocumentBagOfWords = {
    val docId = kryo.readObject(input, classOf[String])
    val wordsArray = kryo.readObject(input, classOf[Array[Int]])
    val countsArray = kryo.readObject(input, classOf[Array[Double]])
    new DocumentBagOfWords(docId.toString, wordsArray, countsArray)
  }
}
