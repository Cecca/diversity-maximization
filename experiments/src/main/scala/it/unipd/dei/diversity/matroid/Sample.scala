package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.SerializationUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

/**
  * Created by ceccarel on 07/08/17.
  */
object Sample {

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Sample")
    val spark = SparkSession.builder()
      .config(sparkConfig)
      .getOrCreate()


    val data =
      if (opts.topics.isDefined) {
        new WikipediaLDAExperiment(spark, opts.input()).loadDataset()
      } else if (opts.genres.isDefined) {
        new SongExperiment(spark, opts.input(), opts.genres()).loadDataset()
      } else if (opts.uniform.isDefined) {
        opts.uniform() match {
          // The value of k==10 is a dummy one, it is not actually used
          case "glove" => new GloVeExperiment(spark, opts.input(), 10).loadDataset()
        }
      } else {
        spark.read.parquet(opts.input())
      }

    val cnt = data.count()
    val sample = data.sample(withReplacement = false, opts.numElements() / cnt.toDouble).cache()
    val sampleSize = sample.count()
    println(s"Sampled $sampleSize elements")
    if (opts.topics.isDefined) {
      sample.write.json(opts.output())
    } else {
      sample.write.parquet(opts.output())
    }

    val meta = SerializationUtils.metadata(opts.input()) ++ Map(
      "sampled-elements" -> sampleSize,
      "original-data" -> opts.input()
    )
    SerializationUtils.writeMetadata(opts.output(), meta)

  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val input = opt[String](required=true)
    lazy val output = opt[String](required=true)
    lazy val numElements = opt[Long](required=true)

    lazy val categories = opt[String]()
    lazy val genres = opt[String]()
    lazy val uniform = opt[String](argName = "DATA TYPE", validate = Set("glove").contains,
                                   descr = "Use a cardinality matroid, and specify the data type")
    lazy val topics = toggle()

  }

}
