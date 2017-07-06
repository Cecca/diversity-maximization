package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopConf

import scala.io.Source


/**
  * Class to hold data from the metrolyrics dataset
  *
  * @see https://www.kaggle.com/gyani95/380000-lyrics-from-metrolyrics
  */
case class Song(index: Long, song: String, artist: String, genre: String, vector: Vector) {
  override def toString: String = s"($index) `$song` $genre"
}

object Song {

  def distance(a: Song, b: Song): Double = MlLibDistances.cosineDistanceFull(a.vector, b.vector)

  def main(args: Array[String]) {
    val opts = new Opts(args)
    opts.verify()

    val experiment = new Experiment()
      .tag("input", opts.input())

    val conf = new SparkConf(true).setAppName("Song statistics")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    val data = spark.read.parquet(opts.input()).as[Song].cache()

    if (opts.genres()) {
      val genres = data.map(_.genre).rdd.countByValue()
      for ((g, cnt) <- genres) {
        experiment.append("genres", jMap(
          "genre" -> g,
          "count" -> cnt
        ))
      }
    }

    if (opts.distances()) {
      experiment.tag("sample-size", opts.sampleSize())

      val totalCount = data.count()
      val sample = data
        .sample(withReplacement = false, opts.sampleSize().toDouble / totalCount)
        .persist()

      // materialize the samples
      val sampleCnt = sample.count()
      println(s"Samples taken: $sampleCnt")
      require(sampleCnt > 0, "No samples taken!")

      val distances = sample.rdd.cartesian(sample.rdd)
        .flatMap{ case (a, b) =>
          if (a.index < b.index) Iterator(Song.distance(a, b))
          else Iterator.empty
        }
        .repartition(spark.sparkContext.defaultParallelism)
//        .filter { case (a, b) => a.index < b.index }
//        .map { case (a, b) => Song.distance(a, b) }
        .cache()
      distances.count()

      val (distBuckets, distCounts) = distances.histogram(100)

      for ((b, cnt) <- distBuckets.zip(distCounts)) {
        experiment.append("distance distribution",
          jMap(
            "distance" -> b,
            "count" -> cnt))
      }
    }

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile(true)
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val input = opt[String](required=true)

    val genres = toggle(default=Some(false))

    val distances = toggle(default=Some(false))

    val sampleSize = opt[Long](default = Some(1000L))

  }

}

class LyricsExperiment(override val spark: SparkSession,
                       val dataPath: String,
                       val genresPath: String) extends ExperimentalSetup[Song] {
  import spark.implicits._

  override val distance: (Song, Song) => Double = Song.distance

  lazy val genresCounts =
    Source.fromFile(genresPath).getLines().map(s => {
      val Array(g, c) = s.split("\\s+")
      (g, c.toInt)
    }).toMap

  private lazy val rawData: Dataset[Song] = spark.read.parquet(dataPath).as[Song].cache()

  override def loadDataset(): Dataset[Song] = {
    val brGenres = spark.sparkContext.broadcast(genresCounts)
    rawData.filter(song => {
      brGenres.value.getOrElse(song.genre, 0) > 0
    })
  }

  override val matroid: Matroid[Song] = new PartitionMatroid[Song](genresCounts, _.genre)

}