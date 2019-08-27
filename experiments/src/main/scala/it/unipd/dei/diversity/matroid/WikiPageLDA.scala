package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.PerformanceMetrics
import org.apache.spark.sql.functions._
import it.unipd.dei.diversity.mllib.Lemmatizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{LDA, LDAModel, LocalLDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

import scala.util.Random


case class WikiPageLDA(id: Long, title: String, topic: Array[Int], vector: Vector) {
  override def toString: String = s"($id) `$title` ${topic.mkString("[", ", ", "]")}"
}

object WikiPageLDA {

  def distanceOnlyPositiveComponents(aPage: WikiPageLDA, bPage: WikiPageLDA): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    val cos = VectorUtils.cosine(aPage.vector, bPage.vector)
    require(0.0 <= cos && cos <= 1.0, s"Cosine out of range: $cos")
    val dist = VectorUtils.TWO_OVER_PI * math.acos(cos)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity, "Points at infinite distance!!")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

  def distanceArbitraryComponents(aPage: WikiPageLDA, bPage: WikiPageLDA): Double = {
    PerformanceMetrics.distanceFnCounterInc()
    val cos = VectorUtils.cosine(aPage.vector, bPage.vector)
    require(-1.0 <= cos && cos <= 1.0, s"Cosine out of range: $cos")
    val dist = VectorUtils.ONE_OVER_PI * math.acos(cos)
    require(dist != Double.NaN, "Distance NaN")
    require(dist < Double.PositiveInfinity, "Points at infinite distance!!")
    require(dist > Double.NegativeInfinity, "Points at negative infinite distance!!")
    dist
  }

  private def loadCounts(data: DataFrame, opts: Opts, spark: SparkSession): DataFrame = {
    val lemmatized = new Lemmatizer()
      .setInputCol("text")
      .setOutputCol("lemmas")
      .transform(data)

    val cleaned = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("tokens")
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
      .transform(lemmatized)

    val vectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("counts")
      .setVocabSize(5000)
      .fit(cleaned)

    vectorizer
      .transform(cleaned)
      .cache()
  }

  private def train(counts: DataFrame, opts: Opts, spark: SparkSession, modelPath: String): LDAModel = {
    require(opts.input.isDefined)
    require(opts.numTopics.isDefined)

    val model = new LDA()
      .setK(opts.numTopics())
      .setMaxIter(10)
      .setFeaturesCol("counts")
      .fit(counts)

    println(s"Local model? ${model.getClass}")
    model.save(modelPath)
    model
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    opts.verify()
    val conf = new SparkConf(true).setAppName("Song statistics")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val gloveMap = spark.sparkContext.broadcast(new GloVeMap(opts.glove()))

    val textData = spark.read.json(opts.input())
    val counts = loadCounts(textData, opts, spark)
    val ldaModelFile = s"${opts.input()}.lda"
    val model = if (FileSystem.get(spark.sparkContext.hadoopConfiguration).exists(new Path(ldaModelFile))) {
      LocalLDAModel.load(ldaModelFile)
    } else {
      train(counts, opts, spark, ldaModelFile)
    }
    val transformed = transform(counts, model, 0.1)
    transformed.show(10)
    val pages = transformed.map({row =>
      val title = row.getString(row.fieldIndex("title"))
      val identifier = row.getString(row.fieldIndex("id")).toInt
      val text = row.getString(row.fieldIndex("text"))
      val topics = row.getSeq[Int](row.fieldIndex("topic")).toArray
      val vector = Array.ofDim[Double](gloveMap.value.dimension)
      var cnt = 0
      for (word <- text.split(' ')) {
        val lowerCase = word.toLowerCase()
        gloveMap.value.apply(lowerCase) match {
          case None => // do nothing
          case Some(v) =>
            for (i <- 0 until vector.length) {
              vector(i) += v(i)
            }
            cnt += 1
        }
      }
      for (i <- 0 until vector.length) {
        vector(i) /= cnt
      }

      WikiPageLDA(
        identifier,
        title,
        topics,
        Vectors.dense(vector)
      )
    })

    pages.write.parquet(opts.output())
  }

  private def transform(counts: DataFrame, model: LDAModel, threshold: Double): DataFrame = {
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

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val input = opt[String](required=true)
    val output = opt[String](required = true)
    val glove = opt[String](required = true, descr = "The path to the Glove word map")
    val numTopics = opt[Int](descr = "The number of topics for the LDA model")
  }
}

class WikipediaLDAExperiment(override val spark: SparkSession,
                             val dataPath: String) extends ExperimentalSetup[WikiPageLDA] {
  import spark.implicits._

  override val distance: (WikiPageLDA, WikiPageLDA) => Double = WikiPageLDA.distanceArbitraryComponents

  private lazy val rawData: Dataset[WikiPageLDA] =
    spark.read.parquet(dataPath)
      .select("id", "title", "topic", "vector")
      .as[WikiPageLDA].cache()

  lazy val topics: Array[Int] = rawData.select("topic").as[Seq[Int]].flatMap(identity).distinct().collect()

  override def loadDataset(): Dataset[WikiPageLDA] =
    rawData
      .mapPartitions(Random.shuffle(_))
      .persist(StorageLevel.MEMORY_ONLY)

  override def pointToMap(point: WikiPageLDA): Map[String, Any] = Map(
    "id" -> point.id,
    "title" -> point.title,
    "topics" -> point.topic
  )

  override val matroid: Matroid[WikiPageLDA] =
    new TransversalMatroid[WikiPageLDA, Int](topics, _.topic)

}
