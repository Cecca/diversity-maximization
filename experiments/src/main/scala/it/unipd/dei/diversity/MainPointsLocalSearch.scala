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
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainPointsLocalSearch {

  def main(args: Array[String]) = {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val input = opts.input()
    val kList = opts.target().split(",").map{_.toInt}
    val epsilonList = opts.epsilon().split(",").map{_.toDouble}
    val runs = opts.runs()
    val approxRuns = opts.approxRuns()

    val distance: (Point, Point) => Double = Distance.euclidean
    val diversity: (IndexedSeq[Point], (Point, Point) => Double) => Double =
      Diversity.clique[Point]

    // Set up Spark lazily, it will be initialized only if the algorithm needs it.
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    lazy val sc = new SparkContext(sparkConfig)

    // Cycle through parameter configurations
    for {
      r <- 0 until runs
      k <- kList
      epsilon  <- epsilonList
    } {
      val experiment = new Experiment()
        .tag("experiment", "Points")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("git-branch", BuildInfo.gitBranch)
        .tag("k", k)
        .tag("epsilon", epsilon)
        .tag("computeFarthest", false)
        .tag("computeMatching", true)
      val metadata = SerializationUtils.metadata(input)
      for ((k, v) <- metadata) {
        experiment.tag(k, v)
      }

      val coreset: Coreset[Point] = {
        val parallelism = sc.defaultParallelism
        experiment.tag("parallelism", parallelism)
        val points = SerializationUtils.sequenceFile(sc, input, parallelism)
        Algorithm.localSearch(
          Partitioning.random(points, experiment),
          k, epsilon, distance, diversity, experiment)
      }

      Approximation.approximate(
        coreset, k, distance, computeFarthest = false, computeMatching = true, approxRuns, experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val target = opt[String](required = true)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val epsilon = opt[String](default = Some("1.0"))

    lazy val input = opt[String](required = true)
  }


}
