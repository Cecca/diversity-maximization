package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.source.{MaterializedPointSource, PointSource, PointSourceRDD}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import ExperimentUtil._

import scala.collection.mutable

object MainSpark {

  def run(sc: SparkContext,
          sourceName: String,
          dim: Int,
          n: Int,
          kernelSize: Int,
          k: Int,
          distance: (Point, Point) => Double,
          computeFarthest: Boolean,
          computeMatching: Boolean,
          dataDir: String,
          experiment: Experiment) = {
    require(kernelSize >= k)

    val distance: (Point, Point) => Double = Distance.euclidean

    val parallelism = sc.defaultParallelism

    println("Read input")
    val input = sc.objectFile[Point](
      DatasetGenerator.filename(dataDir, sourceName, dim, n, k),
      parallelism)

    println("Run!!")
    val (points, mrTime) = timed {
      input.mapPartitions { points =>
        val pointsArr: Array[Point] = points.toArray
        val coreset = MapReduceCoreset.run(
          pointsArr,
          kernelSize,
          k,
          distance)
        Iterator(coreset)
      }.reduce { (a, b) =>
        (a ++ b).distinct
      }
    }

    println("Build results")
    val (farthestSubset, farthestSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeFarthest) {
        timed {
          Some(FarthestPointHeuristic.run(points, k, distance))
        }
      } else {
        (None, 0)
      }

    val (matchingSubset, matchingSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeMatching) {
        timed {
          Some(MatchingHeuristic.run(points, k, distance))
        }
      } else {
        (None, 0)
      }

    approxTable(farthestSubset, matchingSubset, distance).foreach { row =>
      experiment.append("approximation", row)
    }

    val reportTimeUnit = TimeUnit.MILLISECONDS
    experiment.append("performance",
      jMap(
        "farthest-time"  -> convertDuration(farthestSubsetTime, reportTimeUnit),
        "matching-time"  -> convertDuration(matchingSubsetTime, reportTimeUnit),
        "mapreduce-time" -> convertDuration(mrTime, reportTimeUnit)
      ))

  }

  def main(args: Array[String]) {
    val opts = new PointsExperimentConf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.delegates().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val kernelSizeList = opts.kernelSize().split(",").map{_.toInt}
    val runs = opts.runs()
    val materialize = opts.materialize()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val directory = opts.directory()

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    val sc = new SparkContext(sparkConfig)
    for {
      r <- 0 until runs
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      kernSize <- kernelSizeList
    } {
      val experiment = new Experiment()
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("parallelism", sc.defaultParallelism)
        .tag("source", sourceName)
        .tag("space-dimension", dim)
        .tag("k", k)
        .tag("num-points", n)
        .tag("kernel-size", kernSize)
        .tag("algorithm", "MapReduce")
        .tag("materialize", materialize)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)
      run(
        sc, sourceName, dim, n, kernSize, k, Distance.euclidean,
        computeFarthest, computeMatching, directory, experiment)
      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

    sc.stop()
  }

}
