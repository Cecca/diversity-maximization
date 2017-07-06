package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.SerializationUtils
import it.unipd.dei.diversity.mllib.TfIdf
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{StopWordsRemover, Word2VecModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.{ScallopConf, Subcommand}

object Vectorize {

  // Set up Spark lazily, it will be initialized only if the algorithm needs it.
  lazy val spark = {
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Vectorizer")
    val _s = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()
    _s.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    _s
  }

  def loadInput(path: String): DataFrame = {
    val fmt = if(path.endsWith(".json")) "json" else "parquet"
    val dataset = spark.read.format(fmt).load(path)
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("words")
      .setCaseSensitive(false)
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    stopWordsRemover.transform(dataset).filter("size(words) > 0")
  }

  def main(args: Array[String]) {
    val opts = new Opts(args)

    val (outpath, metadata) =
      opts.subcommand match {
        case Some(cmd@opts.tfIdf) =>
          println("Converting to bag-of-words representation")
          val dataset = loadInput(cmd.input()).cache()

          val minLength = cmd.minLength()
          val tfIdf = new TfIdf()
            .setInputCol("words")
            .setOutputCol("vector")
            .setVocabSize(cmd.vocabulary())
            .fit(dataset)
          val transformed = tfIdf.transform(dataset)
            .filter(row => {
              val v = row.getAs[org.apache.spark.ml.linalg.Vector]("vector")
              v.numNonzeros > 0 && v.numNonzeros >= minLength
            })
            .drop("words")
          transformed.write.parquet(cmd.output())
          (cmd.output(), Map(
            "representation" -> "tf-idf",
            "vocabulary-size" -> cmd.vocabulary(),
            "min-length" -> minLength
          ))


        case Some(cmd@opts.word2vec) =>
          println("Converting to word2vec representation")
          val dataset = loadInput(cmd.input()).cache()

          val model = Word2VecModel.load(cmd.word2vecModel())
          val transformed = model
            .setInputCol("words")
            .setOutputCol("vector")
            .transform(dataset)
            .filter(row => row.getAs[org.apache.spark.ml.linalg.Vector]("vector").numNonzeros > 0)
            .drop("words")
          transformed.write.parquet(cmd.output())
          (cmd.output(), Map(
            "representation" -> "word2vec",
            "model" -> cmd.word2vecModel()
          ))
      }

    SerializationUtils.writeMetadata(outpath, metadata)

  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {

    val tfIdf = new Subcommand("tfIdf") {
      lazy val input = opt[String](required = true)
      lazy val output = opt[String](required = true)
      lazy val vocabulary = opt[Int](default = Some(Int.MaxValue))
      lazy val minLength = opt[Int](default = Some(0))
    }
    addSubcommand(tfIdf)

    val word2vec = new Subcommand("word2vec") {
      lazy val word2vecModel = opt[String](required = true)
      lazy val input = opt[String](required = true)
      lazy val output = opt[String](required = true)

    }
    addSubcommand(word2vec)

    verify()

  }

}
