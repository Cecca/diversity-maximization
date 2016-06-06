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

import scala.collection.{JavaConversions, mutable}

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

  def approxTable(farthestSubset: Option[IndexedSeq[Point]],
                  matchingSubset: Option[IndexedSeq[Point]],
                  distance: (Point, Point) => Double) = {

    val columns = mutable.ArrayBuffer[(String, Any)]()

    farthestSubset.foreach { fs =>
      val edgeDiversity = Diversity.edge(fs, distance)
      val treeDiversity = Diversity.tree(fs, distance)
      columns.append(
        "computed-edge" -> edgeDiversity,
        "computed-tree" -> treeDiversity
      )
    }

    matchingSubset.foreach { ms =>
      val cliqueDiversity = Diversity.clique(ms, distance)
      val starDiversity   = Diversity.star(ms, distance)
      columns.append(
        "computed-clique" -> cliqueDiversity,
        "computed-star"   -> starDiversity
      )
    }

    if (columns.nonEmpty) {
      Some(jMap(columns: _*))
    } else {
      None
    }
  }

  def approxTable(farthestSubsetCenters: Option[IndexedSeq[Point]],
                  farthestSubsetWDelegates: Option[IndexedSeq[Point]],
                  matchingSubset: Option[IndexedSeq[Point]],
                  distance: (Point, Point) => Double) = {

    val columns = mutable.ArrayBuffer[(String, Any)]()

    farthestSubsetCenters.foreach { fs =>
      val edgeDiversity = Diversity.edge(fs, distance)
      columns.append(
        "computed-edge" -> edgeDiversity
      )
    }

    farthestSubsetWDelegates.foreach { fs =>
      val treeDiversity = Diversity.tree(fs, distance)
      columns.append(
        "computed-tree" -> treeDiversity
      )
    }

    matchingSubset.foreach { ms =>
      val cliqueDiversity = Diversity.clique(ms, distance)
      val starDiversity   = Diversity.star(ms, distance)
      columns.append(
        "computed-clique" -> cliqueDiversity,
        "computed-star"   -> starDiversity
      )
    }

    if (columns.nonEmpty) {
      Some(jMap(columns: _*))
    } else {
      None
    }
  }


  def computeApproximations(pointSource: PointSource,
                            farthestSubset: Option[IndexedSeq[Point]],
                            matchingSubset: Option[IndexedSeq[Point]]) = {

    val columns = mutable.ArrayBuffer[(String, Any)]()

    farthestSubset.foreach { fs =>
      val edgeDiversity = Diversity.edge(fs, pointSource.distance)
      val treeDiversity = Diversity.tree(fs, pointSource.distance)
      columns.append(
        "computed-edge" -> edgeDiversity,
        "baseline-edge" -> pointSource.edgeDiversity,
        "ratio-edge"    -> pointSource.edgeDiversity.toDouble / edgeDiversity,
        "computed-tree" -> treeDiversity,
        "baseline-tree" -> pointSource.treeDiversity,
        "ratio-tree"    -> pointSource.treeDiversity.toDouble / treeDiversity
      )
    }

    matchingSubset.foreach { ms =>
      val cliqueDiversity = Diversity.clique(ms, pointSource.distance)
      val starDiversity   = Diversity.star(ms, pointSource.distance)
      columns.append(
        "baseline-clique" -> pointSource.cliqueDiversity,
        "computed-clique" -> cliqueDiversity,
        "ratio-clique"    -> pointSource.cliqueDiversity.toDouble / cliqueDiversity,
        "baseline-star"   -> pointSource.starDiversity,
        "computed-star"   -> starDiversity,
        "ratio-star"      -> pointSource.starDiversity.toDouble / starDiversity
      )
    }

    if (columns.nonEmpty) {
      Some(jMap(columns: _*))
    } else {
      None
    }
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
