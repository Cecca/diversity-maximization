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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

object SubsetSelector {

  def selectSubset(documents: RDD[WikiBagOfWords],
                   query: String,
                   distance: (WikiBagOfWords, WikiBagOfWords) => Double,
                   radius: Double = 1.0)
  : RDD[WikiBagOfWords] = {
    documents.persist(StorageLevel.MEMORY_AND_DISK)
    val center: WikiBagOfWords =
      documents.filter(_.title == query).collect().toSeq match {
        case Seq(bow) => bow
        case Seq() => throw new IllegalArgumentException("The queried page does not exist")
        case seq =>
          println("WARNING: There are multiple pages with the same title")
          println(seq.mkString("\n"))
          seq.head
      }

    documents.filter { bow =>
      distance(bow, center) < radius
    }
  }


  def main(args: Array[String]) {

    val opts = new ScallopConf(args) {
      lazy val dataset = opt[String](required = true)
      lazy val query   = opt[String](required = true)
      lazy val radius  = opt[Double](default = Some(1.0))
    }
    opts.verify()

    val inputDataset = opts.dataset()
    val queryString  = opts.query()
    val queryRadius  = opts.radius()

    val conf = new SparkConf(loadDefaults = true)
      .setAppName("Test SubsetSelector")
    val sc = new SparkContext(conf)
    val input = CachedDataset(sc, inputDataset)
    val filtered = selectSubset(input, queryString, WikiBagOfWords.cosineDistance, queryRadius)
      .persist(StorageLevel.MEMORY_AND_DISK)
    println(filtered.map(bow => s"${bow.title} :: ${bow.categories}").collect().mkString("\n"))
    println(s"Selected a total of ${filtered.count()} documents over ${input.count()}")

  }

}
