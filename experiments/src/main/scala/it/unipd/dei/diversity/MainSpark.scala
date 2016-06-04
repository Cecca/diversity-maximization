package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.source.{MaterializedPointSource, PointSource, PointSourceRDD}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf
import ExperimentUtil._

object MainSpark {

  def run(sc: SparkContext,
          source: PointSource,
          kernelSize: Int,
          numDelegates: Int,
          distance: (Point, Point) => Double,
          computeFarthest: Boolean,
          computeMatching: Boolean,
          experiment: Experiment) = {
    println("Create input")
    val input = source match {
      case mat: MaterializedPointSource =>
        println("Using sc.parallelize")
        sc.parallelize(mat.allPoints, sc.defaultParallelism)
      case src => new PointSourceRDD(sc, src, sc.defaultParallelism)
    }

    println("Run!!")
    val parallelism = sc.defaultParallelism
    val localKernelSize = (kernelSize/parallelism.toDouble).toInt
    require(localKernelSize > 0,
      "Should have at least one kernel point per partition")
    val (points, mrTime) = timed {
      input.mapPartitions { points =>
        val pointsArr: Array[Point] = points.toArray
        val coreset = MapReduceCoreset.run(
          pointsArr,
          localKernelSize,
          numDelegates,
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
          Some(FarthestPointHeuristic.run(points, source.k, source.distance))
        }
      } else {
        (None, 0)
      }

    val (matchingSubset, matchingSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeMatching) {
        timed {
          Some(MatchingHeuristic.run(points, source.k, source.distance))
        }
      } else {
        (None, 0)
      }

    computeApproximations(source, farthestSubset, matchingSubset).foreach { row =>
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
      val source =
        if (materialize) {
          PointSource(sourceName, dim, n, k, Distance.euclidean).materialize()
        } else {
          PointSource(sourceName, dim, n, k, Distance.euclidean)
        }
      run(
        sc, source, kernSize, k, Distance.euclidean,
        computeFarthest, computeMatching, experiment)
      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

    sc.stop()
  }

}
