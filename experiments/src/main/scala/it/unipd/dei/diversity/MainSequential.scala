package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.experiment.Experiment

object MainSequential {

  def run(sourceName: String,
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

    println("Read input")
    val input = SerializationUtils.sequenceFile(
      DatasetGenerator.filename(dataDir, sourceName, dim, n, k)).toVector

    println("Run!!")

    val (farthestSubset, farthestSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeFarthest) {
        timed {
          Some(FarthestPointHeuristic.run(input, k, distance))
        }
      } else {
        (None, 0)
      }

    val (matchingSubset, matchingSubsetTime): (Option[IndexedSeq[Point]], Long) =
      if (computeMatching) {
        timed {
          Some(MatchingHeuristic.run(input, k, distance))
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
        "matching-time"  -> convertDuration(matchingSubsetTime, reportTimeUnit)
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
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val directory = opts.directory()

    for {
      r <- 0 until runs
      sourceName <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
      kernSize <- kernelSizeList
    } {
      val experiment = new Experiment()
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("source", sourceName)
        .tag("space-dimension", dim)
        .tag("k", k)
        .tag("num-points", n)
        .tag("kernel-size", kernSize)
        .tag("algorithm", "Sequential")
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)
      run(
        sourceName, dim, n, kernSize, k, Distance.euclidean,
        computeFarthest, computeMatching, directory, experiment)
      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

}
