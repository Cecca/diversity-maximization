package it.unipd.dei.diversity.words

import it.unipd.dei.diversity.{Distance, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class BagOfWordsDataset(val documentsFile: String,
                        val vocabularyFile: String) {

  lazy val wordMap: Map[Int, String] = {
    val lines = Source.fromFile(vocabularyFile).getLines()
    lines.zipWithIndex.map(_.swap).toMap
  }

  def documents(sc: SparkContext): RDD[UCIBagOfWords] =
    sc.textFile(documentsFile).flatMap { line =>
      val tokens = line.split(" ")
      if (tokens.length != 3) {
        Iterator.empty
      } else {
        Iterator((tokens(0).toInt, (tokens(1).toInt, tokens(2).toInt)))
      }
    }.groupByKey().map { case (docId, wordCounts) =>
      new UCIBagOfWords(docId, wordCounts.toMap)
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

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val dataset = new BagOfWordsDataset(documentsFile, vocabularyFile)

    val sample = dataset.documents(sc).take(3)

    sample.foreach { doc =>
      println("=====")
      println(doc.toString(dataset.wordMap))
    }

    Utils.pairs(sample).foreach { case (a, b) =>
      println(s"Pair ${a.documentId}, ${b.documentId}: ${Distance.jaccard(a, b)}")
    }
  }

}
