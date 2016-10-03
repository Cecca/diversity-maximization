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

package it.unipd.dei.diversity.wiki

import it.unipd.dei.diversity.{Algorithm, Approximation, _}
import it.unipd.dei.experiment.Experiment
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object MainWiki {

  def wikiBowToMap(bow: WikiBagOfWords) = Map(
    "title" -> bow.title,
    "categories" -> bow.categories.mkString(",")
  )

  def main(args: Array[String]) {

    // Read command line options
    val opts = new Conf(args)
    opts.verify()
    val dataset = opts.dataset()
    val algorithm = opts.algorithm()
    val kList = opts.delegates().split(",").map{_.toInt}
    // The kernel size list is actually optional
    val kernelSizeList: Seq[Option[Int]] =
      opts.kernelSize.get.map { arg =>
        arg.split(",").map({x => Some(x.toInt)}).toSeq
      }.getOrElse(Seq(None))
    val runs = opts.runs()
    val approxRuns = opts.approxRuns()
    val computeFarthest = opts.farthest()
    val computeMatching = opts.matching()
    val queryTitle = opts.queryTitle.get
    val queryRadius = opts.queryRadius()

    val distance: (WikiBagOfWords, WikiBagOfWords) => Double = WikiBagOfWords.cosineDistance

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("Wikipedia MapReduce coresets")
    val sc = new SparkContext(sparkConfig)

    for {
      r        <- 0 until runs
      k        <- kList
      kernSize <- kernelSizeList
    } {
      println(
        s"""
           |Experiment on $dataset
           |  k  = $k
           |  k' = $kernSize
        """.stripMargin)
      val experiment = new Experiment()
        .tag("experiment", "Wikipedia")
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

      val parallelism = sc.defaultParallelism
      experiment.tag("parallelism", parallelism)
      val documents = CachedDataset(sc, dataset).persist(StorageLevel.MEMORY_AND_DISK)
      val filteredDocuments = queryTitle match {
        case Some(title) =>
          experiment.tag("query-title", queryTitle)
          experiment.tag("query-radius", queryRadius)
          SubsetSelector.selectSubset(documents, title, distance, queryRadius)
            .persist(StorageLevel.MEMORY_AND_DISK)
        case None => documents
      }
      val docCount = filteredDocuments.count()
      println(s"Working on $docCount documents over ${documents.count()}")
      documents.unpersist()
      val coreset: Coreset[WikiBagOfWords] = algorithm match {
        case "mapreduce" =>
          if(kernSize.isEmpty) {
            throw new IllegalArgumentException("Should specify kernel size on the command line")
          }
          Algorithm.mapReduce(filteredDocuments.glom(), kernSize.get, k, distance, experiment)
        case "random" => Algorithm.random(filteredDocuments, k, distance, experiment)
      }

      // Display coreset on console
      println(coreset.points.map(bow => s"${bow.title} :: ${bow.categories}").mkString("\n"))

      Approximation.approximate(
        coreset, k, kernSize.getOrElse(k)*parallelism,
        distance, computeFarthest, computeMatching,
        approxRuns, Some(wikiBowToMap _), experiment)

      experiment.saveAsJsonFile()
      println(experiment.toSimpleString)
    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val dataset = opt[String](required = true)

    lazy val delegates = opt[String](required = true)

    lazy val kernelSize = opt[String](required = false)

    lazy val runs = opt[Int](default = Some(1))

    lazy val approxRuns = opt[Int](default = Some(1))

    lazy val queryTitle = opt[String]()

    lazy val queryRadius = opt[Double](default=Some(1.0))

    lazy val algorithm = opt[String](default = Some("mapreduce"))

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
