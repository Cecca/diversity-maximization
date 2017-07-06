package it.unipd.dei.diversity.matroid

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Dataset, SparkSession}

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

}

class LyricsExperiment(override val spark: SparkSession,
                       val dataPath: String,
                       val genresPath: String) extends ExperimentalSetup[Song] {

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