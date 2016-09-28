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

import it.unipd.dei.diversity.words.{BagOfWordsDataset, DocumentBagOfWords}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainBagOfWords {

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val algorithm = opts.algorithm()
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
    val datasets = opts.dataset().split(",")

    val distance: (DocumentBagOfWords, DocumentBagOfWords) => Double = ArrayBagOfWords.cosineDistance

    // Set up Spark lazily, it will be initialized only if the algorithm needs it.
    lazy val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    lazy val sc = new SparkContext(sparkConfig)

    // Cycle through parameter configurations
    for {
      r        <- 0 until runs
      dataset  <- datasets
      k        <- kList
      kernSize <- kernelSizeList
    } {
      val experiment = new Experiment()
        .tag("experiment", "BagOfWords")
        .tag("version", BuildInfo.version)
        .tag("git-revision", BuildInfo.gitRevision)
        .tag("git-revcount", BuildInfo.gitRevCount)
        .tag("git-branch", BuildInfo.gitBranch)
        .tag("k", k)
        .tag("dataset", dataset)
        .tag("computeFarthest", computeFarthest)
        .tag("computeMatching", computeMatching)

      for (ks <- kernSize) {
        experiment.tag("kernel-size", ks)
      }

      val data = new BagOfWordsDataset(dataset)

      val coreset: Coreset[DocumentBagOfWords] = algorithm match {

        case "mapreduce" =>
          val parallelism = sc.defaultParallelism
          experiment.tag("parallelism", parallelism)
          val input = Partitioning.shuffle(
            data.documents(sc, sc.defaultParallelism), experiment)
          Algorithm.mapReduce(input, kernSize.get, k, distance, experiment)

        case "streaming" =>
          val input = Partitioning.shuffle(
            data.documents(sc, sc.defaultParallelism), experiment)
          Algorithm.streaming(input.toLocalIterator, k, kernSize.get, distance, experiment)

        case "sequential" =>
          val input = data.documents()
          Algorithm.sequential(input.toVector, experiment)

        case "random" =>
          val input = data.documents(sc, sc.defaultParallelism)
          Algorithm.random(input, k, distance, experiment)

      }

      Approximation.approximate[DocumentBagOfWords](
        coreset, k, distance, computeFarthest, computeMatching, approxRuns, Some(formatDocument _), experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  def formatDocument(d: DocumentBagOfWords): Map[String, String] = Map(
    "id" -> d.documentId,
    "words" -> d.toLongString
  )

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val algorithm = opt[String](default = Some("sequential"))

    lazy val dataset = opt[String](required = true)

    lazy val target = opt[String](required = true)

    lazy val kernelSize = opt[String](required = false)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

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
