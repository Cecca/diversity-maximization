package it.unipd.dei.diversity

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf
import org.apache.spark.sql.functions._

object MainLDA {

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("LDA")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val data = spark.read.parquet(opts.input()).cache()

    val cleaned = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("tokens")
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
      .transform(data)

    val vectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("counts")
      .setVocabSize(opts.vocabularySize())
      .fit(cleaned)

    val counts = vectorizer
      .transform(cleaned)
      .cache()

    val model = new LDA()
      .setK(opts.numTopics())
      .setMaxIter(opts.numIterations())
      .setFeaturesCol("counts")
      .fit(counts)

    val vocab = vectorizer.vocabulary

//    model.write.overwrite().save(opts.output())

    val topics = model.describeTopics(3)
    val topicsWithTerms = topics
      .withColumn("terms", udf({indices: Seq[Int] => indices.map({i => vocab(i)})}).apply(topics.col("termIndices")))
    println("Topics")
    topicsWithTerms.select("topic", "terms").show(false)

    val transformed = model.transform(counts)
    transformed.select("title", "topicDistribution").show(false)
    val bestTopic = udf({ topics: org.apache.spark.ml.linalg.Vector =>
      topics.argmax
    }).apply(transformed.col("topicDistribution"))
    val withTopic = transformed.withColumn("topic", bestTopic)//.drop("topicDistribution")
    withTopic.select("title", "topic", "topicDistribution")//.show(false)
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val input = opt[String](required=true, descr="path to a file containing lemmatized text")

    lazy val output = opt[String](required=true, descr="path to which save the output")

    lazy val numTopics = opt[Int](required=true, descr="number of topics to seek")

    lazy val numIterations = opt[Int](default=Some(10))

    lazy val vocabularySize = opt[Int](default=Some(5000))

  }

}
