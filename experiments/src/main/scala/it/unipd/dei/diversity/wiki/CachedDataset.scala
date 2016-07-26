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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import TfIdfTransformation._
import org.apache.spark.storage.StorageLevel

object CachedDataset {

  def cachedFilename(path: String): String =
    path + ".cache"

  def hasCache(sc: SparkContext, path: String): Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.exists(new Path(cachedFilename(path)))
  }

  def apply(sc: SparkContext, path: String): RDD[WikiBagOfWords] = {
    if (hasCache(sc, path)) {
      println(s"Loading dataset $path from cache")
      sc.objectFile[WikiBagOfWords](cachedFilename(path))
    } else {
      println(s"Dataset $path not found in cache, loading from text")
      val input = sc.textFile(path, sc.defaultParallelism)
        .repartition(sc.defaultParallelism)
        .flatMap(WikiBagOfWords.fromLine) // Filter out `None` results
        .rescaleTfIdf()
        .persist(StorageLevel.MEMORY_AND_DISK)
      input.saveAsObjectFile(cachedFilename(path))
      input
    }
  }

}
