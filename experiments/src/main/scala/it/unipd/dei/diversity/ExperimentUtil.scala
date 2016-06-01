/*
 * gradias: distributed graph algorithms
 * Copyright (C) 2013-2015 Matteo Ceccarello
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipd.dei.diversity

import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.source.PointSource

import scala.collection.JavaConversions

object ExperimentUtil {

  def jMap(tuples: (String, Any)*): java.util.Map[String, Object] =
    JavaConversions.mapAsJavaMap(tuples.map(fixTuple).toMap)

  def fixTuple(tuple: (Any, Any)): (String, Object) = {
    val second = tuple._2 match {
      case obj: java.lang.Object => obj
      case v: Boolean => v: java.lang.Boolean
      case c: Char => c: java.lang.Character
      case v: Byte => v: java.lang.Byte
      case v: Short => v: java.lang.Short
      case v: Int => v: java.lang.Integer
      case v: Float => v: java.lang.Float
      case v: Double => v: java.lang.Double
    }
    (tuple._1.toString, second)
  }

  def computeApproximations(pointSource: PointSource,
                            farthestSubset: IndexedSeq[Point],
                            matchingSubset: IndexedSeq[Point]) = {
    val edgeDiversity   = Diversity.edge(farthestSubset, pointSource.distance)
    val cliqueDiversity = Diversity.clique(matchingSubset, pointSource.distance)
    val treeDiversity   = Diversity.tree(farthestSubset, pointSource.distance)
    val starDiversity   = Diversity.star(matchingSubset, pointSource.distance)

    jMap(
      "certificate-edge"   -> pointSource.edgeDiversity,
      "certificate-clique" -> pointSource.cliqueDiversity,
      "certificate-tree"   -> pointSource.treeDiversity,
      "certificate-star"   -> pointSource.starDiversity,
      "computed-edge"      -> edgeDiversity,
      "computed-clique"    -> cliqueDiversity,
      "computed-tree"      -> treeDiversity,
      "computed-star"      -> starDiversity,
      "ratio-edge"         -> pointSource.edgeDiversity.toDouble / edgeDiversity,
      "ratio-clique"       -> pointSource.cliqueDiversity.toDouble / cliqueDiversity,
      "ratio-tree"         -> pointSource.treeDiversity.toDouble / treeDiversity,
      "ratio-star"         -> pointSource.starDiversity.toDouble / starDiversity
    )
  }

  def timed[T](fn: => T): (T, Long) = {
    val start = System.nanoTime()
    val res = fn
    val end = System.nanoTime()
    (res, end - start)
  }

  def convertDuration(duration: Double, unit: TimeUnit): Double =
    duration / unit.toNanos(1)

}
