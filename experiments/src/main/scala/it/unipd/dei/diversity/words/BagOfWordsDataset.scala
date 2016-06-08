package it.unipd.dei.diversity.words

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

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

  def documents(): Iterator[UCIBagOfWords] = {
    val source = Source.fromInputStream(
      new GZIPInputStream(new FileInputStream(documentsFile)))
    val (iterator, last) =
      source.getLines().flatMap { line =>
        val tokens = line.split(" ")
        if (tokens.length != 3) {
          Iterator.empty
        } else {
          Iterator((tokens(0).toInt, tokens(1).toInt, tokens(2).toInt))
        }
      }.foldLeft[(Iterator[UCIBagOfWords], Option[UCIBagOfWords])]((Iterator.empty, None)) {
        case ((it, None), (docId, word, count)) =>
          (it, Some(new UCIBagOfWords(docId, Map(word -> count))))
        case ((it, Some(bow)), (docId, word, count)) =>
          if (docId == bow.documentId) {
            (it, Some(new UCIBagOfWords(docId, bow.wordCounts.updated(word, count))))
          } else {
            (it ++ Iterator(bow), Some(new UCIBagOfWords(docId, Map(word -> count))))
          }
      }
    source.close()
    iterator ++ Iterator(last.get)
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
