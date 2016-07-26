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

import org.scalameter.api._

import scala.collection.mutable
import scala.util.Random

object DistanceBenchmark extends Bench.OfflineReport {

  val vocabulary: Vector[Int] = (0 until 10000).toVector

  val sizes = Gen.exponential("size")(8, 8192, 2)
  val mapPairs = for {
    size <- sizes
  } yield (BOWBuilders.mapBOW(vocabulary, size), BOWBuilders.mapBOW(vocabulary, size))
  val arrayPairs = for {
    size <- sizes
  } yield (BOWBuilders.arrayBOW(vocabulary, size), BOWBuilders.arrayBOW(vocabulary, size))

  performance of "Distance.euclidean" in {

    measure method "MapBOW" in {
      using(mapPairs) in { case (a, b) =>
        Distance.euclidean(a, b)
      }
    }
    measure method "ArrayBOW" in {
      using(arrayPairs) in { case (a, b) =>
        ArrayBagOfWords.euclidean(a, b)
      }
    }

  }

}

object BOWBuilders {

  def randomTable(vocabulary: Vector[Int], size: Int) =
    mutable.HashMap(
      Random.shuffle(vocabulary).take(size).map { w =>
        (w, Random.nextInt(1024).toDouble)
      } :_*)

  def mapBOW(vocabulary: Vector[Int], size: Int): MapBagOfWords[Int] =
    new MapBagOfWords[Int](randomTable(vocabulary, size))

  def arrayBOW(vocabulary: Vector[Int], size: Int): ArrayBagOfWords =
    ArrayBagOfWords(randomTable(vocabulary, size).toSeq)
}
