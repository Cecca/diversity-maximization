// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.diversity.ExperimentUtil.jMap
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainPoints {

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val algorithm = opts.algorithm()
    val input = opts.input()
    val kList = opts.target().split(",").map{_.toInt}
    // The kernel size list is actually optional
    val kernelSizeList: Seq[Option[Int]] =
      opts.kernelSize.get.map { arg =>
        arg.split(",").map({x => Some(x.toInt)}).toSeq
      }.getOrElse(Seq(None))
    val runs = opts.runs()
    val approxRuns = opts.approxRuns()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val directory = opts.directory()
    val partitioning = opts.partitioning()

    val distance: (Point, Point) => Double = Distance.euclidean

    // Set up Spark lazily, it will be initialized only if the algorithm needs it.
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    SerializationUtils.configSerialization(sparkConfig)
    lazy val sc = new SparkContext(sparkConfig)

    // Cycle through parameter configurations
    for {
      r <- 0 until runs
      k        <- kList
      kernSize <- kernelSizeList
    } {
      println(
        s"""
          |Experiment on $input with:
          |  k  = $k
          |  k' = $kernSize
        """.stripMargin)
      
      val experiment = new Experiment()
        .tag("experiment", "Points")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("git-branch", BuildInfo.gitBranch)
        .tag("k", k)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)
      if (kernSize.nonEmpty) {
        experiment.tag("kernel-size", kernSize.get)
      }
      val metadata = SerializationUtils.metadata(input)
      for ((k, v) <- metadata) {
        experiment.tag(k, v)
      }

      val dim = metadata("data.dimension").toInt
      val n = metadata("data.num-points").toInt

      val parallelism = algorithm match {
        case "mapreduce" => sc.defaultParallelism
        case _ => 1
      }

      val coreset: Coreset[Point] = algorithm match {

        case "mapreduce" =>
          if(kernSize.isEmpty) {
            throw new IllegalArgumentException("Should specify kernel size on the command line")
          }
          experiment.tag("parallelism", parallelism)
          experiment.tag("partitioning", partitioning)
          val inputPoints = SerializationUtils.sequenceFile(sc, input, parallelism)
          val points = partitioning match {
            case "random"  => Partitioning.random(inputPoints, experiment)
            case "polar2D" => Partitioning.polar2D(inputPoints, experiment)
            case "grid"    => Partitioning.grid(inputPoints, experiment)
            case "unit-grid"  => Partitioning.unitGrid(inputPoints, experiment)
            case "radius"  => Partitioning.radius(inputPoints, Point.zero(dim), distance, experiment)
            case "radius-old"  => Partitioning.radiusOld(inputPoints, Point.zero(dim), distance, experiment)
            case err       => throw new IllegalArgumentException(s"Unknown partitioning scheme $err")
          }
          Algorithm.mapReduce(points, kernSize.get, k, distance, experiment)

        case "streaming" =>
          if(kernSize.isEmpty) {
            throw new IllegalArgumentException("Should specify kernel size on the command line")
          }
          val parallelism = sc.defaultParallelism
          val points = Partitioning.shuffle(
            SerializationUtils.sequenceFile(sc, input, parallelism),
            experiment)
            .persist(StorageLevel.MEMORY_AND_DISK)

          val _coreset = Algorithm.streaming(points.toLocalIterator, k, kernSize.get, distance, experiment)
          experiment.append("streaming-implementation",
            jMap(
              "num-merges" -> _coreset.numRestructurings,
              "actual-centers" -> _coreset.kernel.length,
              "threshold" -> _coreset.threshold
            ))
          _coreset

        case "sequential" =>
          val points = SerializationUtils.sequenceFile(input)
          Algorithm.sequential(points.toVector, experiment)

        case "random" =>
          val points = SerializationUtils.sequenceFile(sc, input, sc.defaultParallelism)
          Algorithm.random(points, k, distance, experiment)

      }

      Approximation.approximate(
        coreset, k, kernSize.getOrElse(k)*parallelism,
        distance, computeFarthest, computeMatching, approxRuns,
        Some(pointToRow(distance, dim) _), experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  def pointToRow(distance: (Point, Point) => Double, dim: Int)(p: Point)
  : Map[String, Any] = Map(
    "norm" -> distance(p, Point.zero(dim)),
    "point" -> p.toString
  )

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("sequential"))

    lazy val partitioning = opt[String](default = Some("random"))

    lazy val target = opt[String](required = true)

    lazy val kernelSize = opt[String](required = false)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val directory = opt[String](default = Some("/tmp"))

    lazy val input = opt[String](required = true)

    lazy val farthest = toggle(
      default=Some(true),
      descrYes = "Compute metrics based on the farthest-point heuristic",
      descrNo  = "Don't compute metrics based on the farthest-point heuristic")

    lazy val matching = toggle(
      default=Some(true),
      descrYes = "Compute metrics based on the matching heuristic",
      descrNo  = "Don't compute metrics based on the matching heuristic")

  }


}
