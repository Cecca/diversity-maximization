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

package it.unipd.dei.diversity.words

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.io.Source

class MXMBagOfWordsDataset(val file: String) {

  lazy val wordMap: Map[Int, String] = {
    Source.fromFile(file)
      .getLines
      .dropWhile({l => !l.startsWith("%")})
      .take(1)
      .toSeq
      .head
      .drop(1)
      .split(",")
      .zipWithIndex
      .map{case (w, i) => (i+1, w)}.toMap
  }

  def documents(sc: SparkContext): RDD[DocumentBagOfWords] = {
    val docs = sc.textFile(file).flatMap { line =>
      if (line.startsWith("#") || line.startsWith("%")) {
        Iterator.empty
      } else {
        line.split(",") match {
          case Array(id, _, rest@_*) =>
            val counts = rest.map { wc =>
              wc.split(":") match {
                case Array(w, c) => (w.toInt, c.toDouble)
                case err =>
                  throw new IllegalArgumentException(
                    s"Wrong token ${err.mkString(",")} (line: ${line})")
              }
            }
            Iterator(new DocumentBagOfWords(id, counts))
        }
      }
    }
    docs
  }

}
