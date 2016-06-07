package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.diversity.ExperimentUtil._
import it.unipd.dei.diversity.source.PointSource
import it.unipd.dei.experiment.Experiment
import org.rogach.scallop.ScallopConf

object MainStreaming {

  def run(sourceName: String,
          dim: Int,
          n: Int,
          k: Int,
          distance: (Point, Point) => Double,
          kernelSize: Int,
          computeFarthest: Boolean,
          computeMatching: Boolean,
          directory: String,
          experiment: Experiment) = {
    val coreset = new StreamingCoreset(kernelSize, k, distance)

    val input = SerializationUtils.sequenceFile(
      DatasetGenerator.filename(directory, sourceName, dim, n, k))

    val (_, coresetTime) = timed {
      for (p <- input) {
        coreset.update(p)
      }
    }

    val points = coreset.pointsIterator.toArray

    val (farthestSubsetCenters, _): (Option[IndexedSeq[Point]], Long) =
      if (computeFarthest) {
        timed {
          Some(FarthestPointHeuristic.run(coreset.kernelPointsIterator.toArray[Point], k, distance))
        }
      } else {
        (None, 0)
      }

    val (farthestSubsetWDelegates, farthestSubsetTime): (Option[IndexedSeq[Point]], Long) =
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

    approxTable(farthestSubsetCenters, farthestSubsetWDelegates, matchingSubset, distance).foreach { row =>
      experiment.append("approximation", row)
    }

    val updatesTimer = coreset.updatesTimer.getSnapshot
    val reportTimeUnit = TimeUnit.MILLISECONDS
    experiment.append("performance",
      jMap(
        "throughput"    -> coreset.updatesTimer.getMeanRate,
        "coreset-time"  -> convertDuration(coresetTime, reportTimeUnit),
        "farthest-time" -> convertDuration(farthestSubsetTime, reportTimeUnit),
        "matching-time" -> convertDuration(matchingSubsetTime, reportTimeUnit),
        "update-mean"   -> convertDuration(updatesTimer.getMean, reportTimeUnit),
        "update-stddev" -> convertDuration(updatesTimer.getStdDev, reportTimeUnit),
        "update-max"    -> convertDuration(updatesTimer.getMax, reportTimeUnit),
        "update-min"    -> convertDuration(updatesTimer.getMin, reportTimeUnit),
        "update-median" -> convertDuration(updatesTimer.getMedian, reportTimeUnit)
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

    val pl = new ProgressLogger("experiments")
    pl.displayFreeMemory = true
    pl.expectedUpdates =
      dimList.length*kList.length*numPointsList.length*kernelSizeList.length
    pl.start(s"Starting ${pl.expectedUpdates} experiments")
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
        .tag("source", sourceName)
        .tag("space-dimension", dim)
        .tag("k", k)
        .tag("num-points", n)
        .tag("kernel-size", kernSize)
        .tag("algorithm", "Streaming")
        .tag("materialize", materialize)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)

      run(
        sourceName, dim, n, k, Distance.euclidean, kernSize,
        computeFarthest, computeMatching, directory, experiment)
      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)

      pl.update()
    }
    pl.stop("Done")
  }

}
