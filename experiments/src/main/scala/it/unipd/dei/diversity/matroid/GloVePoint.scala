package it.unipd.dei.diversity.matroid
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopConf

case class GloVePoint(word: String, vector: DenseVector) {
  override def toString: String = word
}

object GloVePoint {
  def distance(a: GloVePoint, b: GloVePoint): Double = MlLibDistances.cosineDistanceFull(a. vector, b.vector)

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    opts.verify()

    if (opts.output.isDefined) {
      println(s"Converting ${opts.input()} to binary format")
      val conf = new SparkConf(true).setAppName("GloVe convert to binary")
      val spark = SparkSession.builder()
        .config(conf)
        .getOrCreate()
      import spark.implicits._
      val data = spark.sparkContext.textFile(opts.input())
        .map({line =>
          val tokens = line.split(" ")
          val word = tokens(0)
          val vector = new DenseVector(tokens.tail.map(_.toDouble))
          GloVePoint(word, vector)
        })
      spark.createDataset(data).write.parquet(opts.output())
    } else {
      println("Nothing to do, quitting.")
    }

  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val input = opt[String](required=true)
    val output = opt[String](required=false)
  }
}

class GloVeExperiment(override val spark: SparkSession,
                      val dataPath: String,
                      val cardinality: Int) extends ExperimentalSetup[GloVePoint] {
  import spark.implicits._

  override val distance: (GloVePoint, GloVePoint) => Double = GloVePoint.distance
  override val matroid: Matroid[GloVePoint] = new CardinalityMatroid[GloVePoint](cardinality)
  override def pointToMap(point: GloVePoint): Map[String, Any] = Map("word" -> point.word)

  private lazy val rawData: Dataset[GloVePoint] = spark.read.parquet(dataPath).as[GloVePoint].cache()

  override def loadDataset(): Dataset[GloVePoint] = rawData
}
