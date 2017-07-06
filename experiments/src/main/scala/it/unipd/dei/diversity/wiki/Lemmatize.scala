package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.mllib.Lemmatizer
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

/**
  * Lemmatize a dataset using the column "text".
  * Input format json, output format parquet.
  */
object Lemmatize {

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val spark = SparkSession.builder()
      .appName("Lemmatize")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val jsonRdd = spark.sparkContext
      .wholeTextFiles(opts.input(), spark.sparkContext.defaultParallelism)
      .flatMap { case (_, text) => text.split("\n") }
    val raw = spark.read.json(jsonRdd)

    val lemmatizer = new Lemmatizer()
      .setInputCol(opts.textColumn())
      .setOutputCol("lemmas")
    val withLemmas = lemmatizer.transform(raw)

    withLemmas.write.parquet(opts.output())
  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = opt[String](required=true)

    val output = opt[String](required=true)

    val textColumn = opt[String](default = Some("text"))

  }

}
