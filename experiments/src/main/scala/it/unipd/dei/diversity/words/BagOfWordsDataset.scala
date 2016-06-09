package it.unipd.dei.diversity.words

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import it.unipd.dei.diversity.{Distance, Utils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.{FileInputFormat, InvalidInputException}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.Source

class BagOfWordsDataset(val documentsFile: String,
                        val vocabularyFile: String) {

  val sparkCacheFile = documentsFile + ".cache"

  def hasSparkCacheFile(sc: SparkContext): Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.exists(new Path(sparkCacheFile))
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

  def documents(): Iterator[UCIBagOfWords] = new Iterator[UCIBagOfWords] {

    val source = Source.fromInputStream(
      new GZIPInputStream(new FileInputStream(documentsFile)))

    val tokenized =
      source.getLines().drop(3).map { line =>
        val tokens = line.split(" ")
        require(tokens.length == 3)
        (tokens(0).toInt, tokens(1).toInt, tokens(2).toInt)
      }

    var first = tokenized.next()

    override def hasNext: Boolean = tokenized.hasNext

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

      new UCIBagOfWords(docId, words.toSeq)
    }

    override def finalize(): Unit = {
      source.close()
    }
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
