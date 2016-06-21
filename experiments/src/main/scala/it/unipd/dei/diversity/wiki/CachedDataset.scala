package it.unipd.dei.diversity.wiki

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
        .map(WikiBagOfWords.fromLine).cache()
      input.saveAsObjectFile(cachedFilename(path))
      input
    }
  }

}