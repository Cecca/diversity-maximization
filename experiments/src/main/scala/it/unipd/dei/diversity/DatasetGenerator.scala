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

import java.io.PrintWriter

import scala.util.Random
import it.unipd.dei.diversity.source.PointSource
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

object DatasetGenerator {

  def filename(dir: String, sourceName: String, dim: Int, n: Int, k: Int, ext: String) =
    s"$dir/$sourceName-$dim-$n-$k.$ext"

  def main(args: Array[String]) = {

    val opts = new Conf(args)
    opts.verify()

    val sourcesList = opts.source().split(",")
    val dimList = opts.spaceDimension().split(",").map{_.toInt}
    val kList = opts.k().split(",").map{_.toInt}
    val numPointsList = opts.numPoints().split(",").map{_.toLong}
    val outputDir = opts.directory()

    val randomGen = new Random()

    for {
      sourceName   <- sourcesList
      dim      <- dimList
      k        <- kList
      n        <- numPointsList
    } {
      val source = PointSource(sourceName, dim, n, k, Distance.euclidean, randomGen)

      val numGenerated = opts.format() match {
        case "text" =>
          val pw = new PrintWriter(filename(outputDir, sourceName, dim, n.toInt, k, "csv"))
          var cnt = 0l
          println(s"Source has ${source.certificate.length} certificate points")
          for (point <- source.iterator) {
            val s = point.data.mkString(",")
            pw.write(s)
            pw.write('\n')
            cnt += 1
          }
          pw.close()
          cnt
        case "sequence" =>
          val sconf = new SparkConf(true).setAppName("Generator")
          val sc = new SparkContext(sconf)
          SerializationUtils.saveAsSequenceFile(sc, source, outputDir)
      }
      require(numGenerated >= n,
        s"Not enough points have been generated! $numGenerated < $n")
      println(s"Generated $numGenerated points")

    }

  }

  class Conf(args: Array[String]) extends ScallopConf(args) {

    lazy val source = opt[String](default = Some("versor"))

    lazy val spaceDimension = opt[String](default = Some("2"))

    lazy val k = opt[String](required = true)

    lazy val numPoints = opt[String](required = true)

    lazy val directory = opt[String](required = true)

    lazy val format = opt[String](default = Some("sequence"))

  }


}
