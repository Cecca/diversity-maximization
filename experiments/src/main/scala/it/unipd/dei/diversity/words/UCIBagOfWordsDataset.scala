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

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.GZIPInputStream

import scala.collection.mutable
import scala.io.Source

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class UCIBagOfWordsDataset(val documentsFile: String,
                           val vocabularyFile: String) {

  val sparkCacheFile = documentsFile + ".cache"
  val streamingCacheFile = documentsFile + ".streaming.cache"

  def hasSparkCacheFile(sc: SparkContext): Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.exists(new Path(sparkCacheFile))
  }

  def hasStreamingCacheFile: Boolean = {
    val file = new File(streamingCacheFile)
    file.exists()
  }

  lazy val wordMap: Map[Int, String] = {
    val lines = Source.fromFile(vocabularyFile).getLines()
    lines.zipWithIndex.map(_.swap).toMap
  }

  def documents(sc: SparkContext): RDD[UCIBagOfWords] = {
    if (hasSparkCacheFile(sc)) {
      println("Loading documents from cache")
      sc.objectFile(sparkCacheFile, sc.defaultParallelism)
    } else{
      println("No cache for requested documents, load from text file")
      val docs = sc.textFile(documentsFile).flatMap { line =>
        val tokens = line.split(" ")
        if (tokens.length != 3) {
          Iterator.empty
        } else {
          Iterator((tokens(0).toInt, (tokens(1).toInt, tokens(2).toDouble)))
        }
      }.groupByKey().map { case (docId, wordCounts) =>
        new UCIBagOfWords(docId, wordCounts.toSeq)
      }.cache()
      docs.saveAsObjectFile(sparkCacheFile)
      docs
    }
  }

  def documents(): Iterator[UCIBagOfWords] =
    if (hasStreamingCacheFile) {
      println(s"Loading stream from cache file $streamingCacheFile")
      new UCICacheFileIterator(streamingCacheFile)
    } else {
      println(s"No streaming cache file found, loading from text file $documentsFile")
      new UCITextFileIterator(documentsFile, streamingCacheFile)
    }
}

object UCIBagOfWordsDataset {

  def fromName(name: String, directory: String) = {
    val docword = s"$directory/docword.$name.txt.gz"
    val vocab   = s"$directory/vocab.$name.txt"
    new UCIBagOfWordsDataset(docword, vocab)
  }

  def main(args: Array[String]) {
    val documentsFile = args(0)
    val vocabularyFile = args(1)

    val dataset = new UCIBagOfWordsDataset(documentsFile, vocabularyFile)

    for (doc <- dataset.documents()) {
      val words = doc.words.map(w => dataset.wordMap.getOrElse(w, w.toString)).toSeq.sorted
      println(s"${doc.documentId}: ${words.mkString(" ")}")
    }
  }

}

class UCIKryoSerializer extends Serializer[UCIBagOfWords] {
  override def write(kryo: Kryo, output: Output, bow: UCIBagOfWords): Unit = {
    kryo.writeObject(output, bow.documentId)
    kryo.writeObject(output, bow.wordsArray)
    kryo.writeObject(output, bow.scoresArray)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[UCIBagOfWords]): UCIBagOfWords = {
    val docId = kryo.readObject(input, classOf[Int])
    val wordsArray = kryo.readObject(input, classOf[Array[Int]])
    val countsArray = kryo.readObject(input, classOf[Array[Double]])
    new UCIBagOfWords(docId, wordsArray, countsArray)
  }
}

class UCICacheFileIterator(val cacheFile: String) extends Iterator[UCIBagOfWords] {

  val kryo = new Kryo()
  val input = new Input(new FileInputStream(cacheFile))
  val serializer = new UCIKryoSerializer()

  override def hasNext: Boolean = input.available() > 0

  override def next(): UCIBagOfWords =
    kryo.readObject(input, classOf[UCIBagOfWords], serializer)

  override def finalize(): Unit = {
    input.close()
  }
}

class UCITextFileIterator(val documentsFile: String,
                          val cacheFile: String)
extends Iterator[UCIBagOfWords] {

  val source = Source.fromInputStream(
    new GZIPInputStream(new FileInputStream(documentsFile)))

  val kryo = new Kryo()
  val cacheOutput = new Output(new FileOutputStream(cacheFile))
  val serializer = new UCIKryoSerializer()

  val tokenized =
    source.getLines().drop(3).map { line =>
      val tokens = line.split(" ")
      require(tokens.length == 3)
      (tokens(0).toInt, tokens(1).toInt, tokens(2).toInt)
    }

  var first = tokenized.next()

  override def hasNext: Boolean = {
    val hn = tokenized.hasNext
    if(!hn) {
      // The output is flushed when there is no next element because
      // relying only on the close in the finalize method does not
      // seem to be enough: when deserializing we would get buffer
      // exception originated from incomplete input
      cacheOutput.flush()
    }
    hn
  }

  override def next(): UCIBagOfWords = {
    val (docId, word, count) = first
    val words = mutable.HashMap[Int, Double](word -> count)
    var current = first
    while(tokenized.hasNext && current._1 == docId) {
      current = tokenized.next()
      if (current._1 == docId) {
        words(current._2) = current._3
      } else {
        first = current
      }
    }

    val bow = new UCIBagOfWords(docId, words.toSeq)
    kryo.writeObject(cacheOutput, bow, serializer)
    bow
  }

  override def finalize(): Unit = {
    if (hasNext) {
      throw new IllegalStateException(
        "The stream should be consumed completely, otherwise caching does not work")
    }
    source.close()
    cacheOutput.close()
  }
}
