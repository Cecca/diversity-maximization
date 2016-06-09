package it.unipd.dei.diversity.words

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.GZIPInputStream

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import it.unipd.dei.diversity.{Distance, Utils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.Source

class BagOfWordsDataset(val documentsFile: String,
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
          Iterator((tokens(0).toInt, (tokens(1).toInt, tokens(2).toInt)))
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

object BagOfWordsDataset {

  def fromName(name: String, directory: String) = {
    val docword = s"$directory/docword.$name.txt.gz"
    val vocab   = s"$directory/vocab.$name.txt"
    new BagOfWordsDataset(docword, vocab)
  }

  def main(args: Array[String]) {
    val documentsFile = args(0)
    val vocabularyFile = args(1)

    //val conf = new SparkConf().setAppName("test").setMaster("local")
    //val sc = new SparkContext(conf)
    val dataset = new BagOfWordsDataset(documentsFile, vocabularyFile)

    val sample = dataset.documents().take(3).toVector

    sample.foreach { doc =>
      println("=====")
      println(doc.toString(dataset.wordMap))
    }

    Utils.pairs(sample).foreach { case (a, b) =>
      println(s"Pair ${a.documentId}, ${b.documentId}: ${Distance.jaccard(a, b)}")
    }
  }

}

class UCIKryoSerializer extends Serializer[UCIBagOfWords] {
  override def write(kryo: Kryo, output: Output, bow: UCIBagOfWords): Unit = {
    kryo.writeObject(output, bow.documentId)
    kryo.writeObject(output, bow.wordsArray)
    kryo.writeObject(output, bow.countsArray)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[UCIBagOfWords]): UCIBagOfWords = {
    val docId = kryo.readObject(input, classOf[Int])
    val wordsArray = kryo.readObject(input, classOf[Array[Int]])
    val countsArray = kryo.readObject(input, classOf[Array[Int]])
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
    val words = mutable.HashMap[Int, Int](word -> count)
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