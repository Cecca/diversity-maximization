package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.source.{PointSource, PointSourceRDD}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import ExperimentUtil._

object MainSpark {

  /**
    * Merge two sorted arrays
    */
  def merge(a: Array[Point], b: Array[Point]): Array[Point] = {
    var i = 0
    var aIdx = 0
    var bIdx = 0

    val result: Array[Point] = Array.ofDim(a.length+b.length)

    // merge the first elements
    while(aIdx < a.length && bIdx < b.length) {
      if (a(aIdx) == b(bIdx)) {
        result(i) = a(aIdx)
        aIdx += 1
        bIdx += 1
      } else if(a(aIdx).compareTo(b(bIdx)) < 0) {
        result(i) = a(aIdx)
        aIdx += 1
      } else {
        result(i) = b(bIdx)
        bIdx += 1
      }
      i += 1
    }

    // merge the remaining
    while(aIdx < a.length) {
      result(i) = a(aIdx)
      aIdx += 1
      i += 1
    }
    while(bIdx < b.length) {
      result(i) = b(bIdx)
      bIdx += 1
      i += 1
    }

    result.take(i)
  }


  def run(sc: SparkContext,
          source: PointSource,
          kernelSize: Int,
          numDelegates: Int,
          distance: (Point, Point) => Double,
          experiment: Experiment) = {
    val input = new PointSourceRDD(sc, source, sc.defaultParallelism)
    val parallelism = sc.defaultParallelism
    val (points, mrTime) = timed {
      input.mapPartitions { points =>
        val pointsArr: Array[Point] = points.toArray
        val coreset = MapReduceCoreset.run(pointsArr,
          kernelSize/parallelism, numDelegates, distance)
        Iterator(coreset.sorted)
      }.reduce { (a, b) =>
        merge(a, b)
      }
    }

    val (farthestSubset, farthestSubsetTime) = timed{
      FarthestPointHeuristic.run(points, source.k, source.distance)
    }
    println(s"Farthest heuristic computed in $farthestSubsetTime nanoseconds")

    val (matchingSubset, matchingSubsetTime) = timed{
      MatchingHeuristic.run(points, source.k, source.distance)
    }
    println(s"Matching heuristic computed in $matchingSubsetTime nanoseconds")

    experiment.append("approximation",
      computeApproximations(source, farthestSubset, matchingSubset))

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

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    val sc = new SparkContext(sparkConfig)
    for {
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      kernSize <- kernelSizeList
    } {
      try {
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
        val source = PointSource(sourceName, dim, n, k, Distance.euclidean)
        run(sc, source, kernSize, k, Distance.euclidean, experiment)
        experiment.saveAsJsonFile()
        println(experiment.toSimpleString)
      } catch {
        case e: Exception =>
          println(s"Error: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    sc.stop()
  }

}
