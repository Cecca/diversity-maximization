package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.PerformanceMetrics
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

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

}

class WikipediaLDAExperiment(override val spark: SparkSession,
                             val dataPath: String) extends ExperimentalSetup[WikiPageLDA] {
  import spark.implicits._

  override val distance: (WikiPageLDA, WikiPageLDA) => Double = WikiPageLDA.distanceArbitraryComponents

  private lazy val rawData: Dataset[WikiPageLDA] = spark.read.parquet(dataPath).as[WikiPageLDA].cache()

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