package it.unipd.dei.diversity.matroid

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopConf

import scala.io.Source


/**
  * Class to hold data from the MusiXMatch dataset.
  * Genres come from the tagtraum dataset.
  *
  * http://millionsongdataset.com/musixmatch/
  * http://www.tagtraum.com/msd_genre_datasets.html
  */
case class Song(trackId: String, genre: String, vector: Vector) {
  override def toString: String = s"($trackId) $genre"
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

    if (opts.output.isDefined) {
      println("Converting dataset")
      val totalSongs = spark.sparkContext.longAccumulator("total songs")
      val missingGenres = spark.sparkContext.longAccumulator("missing genre")
      require(opts.genres.isDefined, "--genres must be provided")
      val genres = spark.sparkContext.broadcast(new GenresMap(opts.genres()))
      val rdd = spark.sparkContext.textFile(opts.input())
        .filter(l => !(l.startsWith("#") || l.startsWith("%")))
        .map({line =>
          totalSongs.add(1)
          val tokens = line.split(',')
          val trackId = tokens(0)
          val genre = genres.value.get(trackId) match {
            case Some(g) => g
            case None =>
              missingGenres.add(1)
              "Unknown"
          }
          val coordinates = tokens.view(2, tokens.length).map({coord =>
            val parts = coord.split(':')
            // We offset by 1 because they start counting from 1 the tokens
            (parts(0).toInt - 1, parts(1).toDouble)
          })
          // 5000 is the size of the vocabulary, as described in the dataset's website
          val vector = Vectors.sparse(5000, coordinates)
          Song(trackId, genre, vector)
        })

      spark.createDataset(rdd).write.parquet(opts.output())

      println(s"Total songs: ${totalSongs.value}, missing genre: ${missingGenres.value}")

      System.exit(0)
    }

    val data = spark.read.parquet(opts.input()).as[Song].cache()

    if (opts.genresHist()) {
      val counts = data.map(song => song.genre).rdd.countByValue().toSeq.sortBy(_._2)
      for ((genre, count) <- counts) {
        println(s"$genre $count")
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
          if (a.trackId < b.trackId) Iterator(Song.distance(a, b))
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

    val genres = opt[String](descr = "Required when --output is provided. Contains the list of genres")

    val genresHist = toggle(default=Some(false))

    val distances = toggle(default=Some(false))

    val sampleSize = opt[Long](default = Some(1000L))

    val output = opt[String](descr = "When provided, converts the file to binary format")
  }

}

class SongExperiment(override val spark: SparkSession,
                       val dataPath: String,
                       val genresPath: String) extends ExperimentalSetup[Song] {
  import spark.implicits._

  override val distance: (Song, Song) => Double = Song.distance

  override def pointToMap(point: Song): Map[String, Any] = Map(
    "trackId" -> point.trackId,
    "genre" -> point.genre
  )

  lazy val genresCounts =
    Source.fromFile(genresPath).getLines().map(s => {
      val Array(g, c) = s.split("\\s+")
      (g.replace("_", " "), c.toInt)
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

private class GenresMap(val path: String) extends Serializable {
  private val map = {
    Source.fromFile(path).getLines()
      .filter(l => !l.startsWith("#"))
      .map({line =>
        val tokens = line.split("\t")
        val trackId = tokens(0)
        val genre = tokens(1) // We ignore the ambiguous genres
        (trackId, genre)
      }).toMap
  }

  println(s"Loaded genres map with ${map.size} songs")

  def get(trackId: String): Option[String] = map.get(trackId)

}