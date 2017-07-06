package it.unipd.dei.diversity.matroid

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object TrainWord2Vec {

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()
    val path = opts.input()

    val spark = SparkSession.builder()
      .appName("WikiStats")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val dataset = spark.read.parquet(path)
      .select("lemmas").sample(false, 0.01).cache()

    val word2VecModel = new Word2Vec()
      .setInputCol("lemmas")
      .setOutputCol("word2vec-vectors")
      .setVectorSize(opts.vectorSize())
      .setMaxIter(opts.maxIter())
      .setNumPartitions(spark.sparkContext.defaultParallelism)
      .fit(dataset)

    // Find synonyms of a word to force the evaluation of the
    // word vectors, otherwise the save operation crashes with
    // a "task too big" exception
    import spark.implicits._
    val sampleWord = word2VecModel.getVectors.select("word").as[String].head()
    word2VecModel.findSynonyms(sampleWord, 10).show()
    word2VecModel.save(opts.output())
  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    val input = opt[String](required = true)

    val output = opt[String](required = true)

    val vectorSize = opt[Int](default = Some(300))

    val maxIter = opt[Int](default = Some(10))

  }


}
