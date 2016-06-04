package it.unipd.dei.diversity.source

import it.unipd.dei.diversity.{Distance, Point}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

class PointSourcePartition(override val index: Int,
                           val size: Int,
                           val certificatePoints: Array[Point],
                           val points: RandomPointIterator)
extends Partition {

  def iterator: Iterator[Point] =
    new InterleavingPointIterator(certificatePoints, points, size)

}


class PointSourceRDD(sc: SparkContext,
                     @transient private val source: PointSource,
                     val numSplits: Int)
extends RDD[Point](sc, Nil) {

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Point] = {
    split.asInstanceOf[PointSourcePartition].iterator
  }

  override protected def getPartitions: Array[Partition] = {
    val pointsPerSplit = math.ceil(source.n.toDouble / numSplits).toInt
    val certPerSplit = math.ceil(source.certificate.length.toDouble / numSplits).toInt
    require(certPerSplit > 0,
      s"certPerSplit should be positive, but ${source.certificate.length} / $numSplits = 0")
    val certParts = Array.fill[Array[Point]](numSplits)(Array.empty[Point])
    var i = 0
    for (group <- source.certificate.grouped(certPerSplit)) {
      certParts(i) = group
      i += 1
    }
    require(certParts.length == numSplits,
      s"Number of certificate parts=${certParts.length}, number of splits $numSplits")
    val parts = certParts.zipWithIndex.map { case (certPart, idx) =>
      new PointSourcePartition(idx, pointsPerSplit, certPart, source.points).asInstanceOf[Partition]
    }
    parts
  }

}

object PointSourceRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val source = PointSource("random-gaussian-sphere", 2, 16, 2, Distance.euclidean)
    println(source.certificate.toSeq)
    val input = new PointSourceRDD(sc, source, 8)

    input.mapPartitions { ps =>
      Iterator(ps.toArray)
    }.collect().foreach { points =>
      println(s"=== Partition ========\n${points.mkString("\n")}")
    }

    val cnt = input.count()
    println(s"Count is $cnt")

    sc.stop()
  }
}
