package it.unipd.dei.diversity.matroid

import java.io.{FileOutputStream, PrintWriter}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{LDA, LDAModel, LocalLDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StopWordsRemover}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.{DefaultParamsWritable, MLWritable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop.ScallopConf

object TrainLDA {

  val stopWords: Array[String] = StopWordsRemover.loadDefaultStopWords("english") ++ Seq(
    "-", "$", "%", "°", "<br>", "also", "|", "!", "?"
  )

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val sparkConf = new SparkConf(loadDefaults = true).setAppName("LDA")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path(opts.model()))) {
      println("Model does not exist, start training")
      train(opts, spark)
    } else if (opts.describe.isDefined) {
      println(s"Print information on topic ${opts.describe()}")
      describe(opts, spark)
    } else if (opts.dump.isDefined) {
      dump(opts, spark)
    } else if (opts.output.isDefined) {
      transform(opts, spark)
    }

  }

  def vocabname(modelName: String) = s"$modelName.vocab"

  def describeWithTerms(topics: DataFrame, vocabulary: Array[String]) = {
    val topicsWithTerms = topics
      .withColumn("terms", udf({ indices: Seq[Int] => indices.map({ i => vocabulary(i) }) }).apply(topics.col("termIndices")))
    println("Topics")
    topicsWithTerms.select("topic", "terms").show(false)
  }

  def loadData(opts: Opts, spark: SparkSession): (DataFrame, CountVectorizerModel) = {
    val data = spark.read.parquet(opts.input()).cache()

    val cleaned = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("tokens")
      .setStopWords(stopWords)
      .transform(data)

    val vectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("counts")
      .setVocabSize(opts.vocabularySize())
      .fit(cleaned)

    val counts = vectorizer
      .transform(cleaned)
      .cache()

    (counts, vectorizer)
  }



  /**
    * Trains the model, saves it, and print some debug output
    *
    * @param opts
    * @param spark
    */
  def train(opts: Opts, spark: SparkSession): Unit = {
    require(opts.input.isDefined)
    require(opts.numTopics.isDefined)
    val (counts, vectorizer) = loadData(opts, spark)


    val model = new LDA()
      .setK(opts.numTopics())
      .setMaxIter(opts.numIterations())
      .setFeaturesCol("counts")
      .fit(counts)

    println(s"Local model? ${model.getClass}")
    model.save(opts.model())
    vectorizer.save(vocabname(opts.model()))

    val vocab = vectorizer.vocabulary

    describeWithTerms(model.describeTopics(6), vocab)

    val withTopic: DataFrame = transform(counts, model, opts.threshold())
    withTopic.select("title", "topic").show(false)
  }

  def transform(counts: DataFrame, model: LDAModel, threshold: Double): DataFrame = {
    val transformed = model.transform(counts)
    val bestTopic = udf({ topics: Vector =>
      topics.toArray
        .zipWithIndex
        .filter { case (score, idx) => score > threshold }
        .map {
          _._2
        }
    }).apply(transformed.col("topicDistribution"))
    val withTopic = transformed.withColumn("topic", bestTopic).drop("topicDistribution")
    withTopic
  }

  def describe(opts: Opts, spark: SparkSession): Unit = {
    val model = LocalLDAModel.load(opts.model())
    val vocabulary = CountVectorizerModel.load(vocabname(opts.model())).vocabulary
    val topics = model.describeTopics
    describeWithTerms(topics.filter(s"topic == ${opts.describe()}"), vocabulary)
  }

  def dump(opts: Opts, spark: SparkSession): Unit = {
    val model = LocalLDAModel.load(opts.model())
    val vocabulary = CountVectorizerModel.load(vocabname(opts.model())).vocabulary
    val topics = model.describeTopics
    val topicsWithTerms = topics
      .withColumn(
        "terms",
        udf({ indices: Seq[Int] => indices.map({ i => vocabulary(i) }) }).apply(topics.col("termIndices")))
    val tops = topicsWithTerms.select("topic", "terms").collect()
    val out = new PrintWriter(new FileOutputStream(opts.dump()))
    for (t <- tops) {
      val id = t.getAs[Int]("topic")
      val words = t.getAs[Seq[String]]("terms").mkString(" ")
      out.println(s"$id  $words")
    }
    out.close()
  }

  def transform(opts: Opts, spark: SparkSession): Unit = {
    require(opts.output.isDefined)
    val model = LocalLDAModel.load(opts.model())
    val (counts, vectorizer) = loadData(opts, spark)
    val transformed = transform(counts, model, opts.threshold())
    transformed.write.parquet(opts.output())
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val input = opt[String](descr="path to a file containing lemmatized text")

    lazy val output = opt[String](descr="path to output file")

    lazy val model = opt[String](required=true, descr="path to the model")

    lazy val numTopics = opt[Int](descr="number of topics to seek")

    lazy val numIterations = opt[Int](default=Some(10))

    lazy val vocabularySize = opt[Int](default=Some(5000))

    lazy val threshold = opt[Double](default=Some(0.1))

    lazy val describe = opt[Int](descr = "describe the given topic")

    lazy val dump = opt[String](descr = "dump the topics to the given text file")

  }

}
