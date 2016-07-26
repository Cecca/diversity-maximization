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

import scala.util.Random

import it.unipd.dei.diversity.source.PointSource
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object DatasetGenerator {

  def filename(dir: String, sourceName: String, dim: Int, n: Int, k: Int) =
    s"$dir/$sourceName-$dim-$n-$k.points"

  def main(args: Array[String]) {

    val opts = new Conf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.k().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toInt}
    val outputDir = opts.outputDir()

    val randomGen = new Random()

    val sparkConfig = new SparkConf(loadDefaults = true)
      .setAppName("MapReduce coresets")
    val sc = new SparkContext(sparkConfig)
    for {
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
    } {
      val source = PointSource(sourceName, dim, n, k, Distance.euclidean, randomGen)
      
//      val rdd = sc.parallelize(source.toVector)
//        .persist(StorageLevel.MEMORY_AND_DISK)

      val numGenerated = SerializationUtils.saveAsSequenceFile(
        source.iterator, filename(outputDir, sourceName, dim, n, k))
      require(numGenerated >= n,
        s"Not enough points have been generated! $numGenerated < $n")
      println(s"Generated $numGenerated points")

//      rdd.saveAsObjectFile(filename(outputDir, sourceName, dim, n, k))

    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val source = opt[String](default = Some("versor"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val k = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

    lazy val outputDir = opt[String](required = true)

  }


}
