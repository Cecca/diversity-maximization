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

import java.io._
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.diversity.source.PointSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, NullWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Reader, Writer}
import org.apache.hadoop.io.compress.DeflateCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.slf4j.LoggerFactory

object SerializationUtils {

  def getKryo: Kryo = {
    val _kryo = new Kryo()
    _kryo.register(classOf[Point], new PointSerializer())
    _kryo
  }

  def sequenceFile(sc: SparkContext, path: String, parallelism: Int): RDD[Point] = {
    val bytes = sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], parallelism)
    bytes.mapPartitions { iter =>
      val kr = getKryo
      iter.map { case (_, bytesArr) =>
        kr.readObject(new Input(bytesArr.copyBytes()), classOf[Point])
      }
    }
  }

  def sequenceFile(file: String): Iterator[Point] = {
    val path = new Path(file)
    val conf = new Configuration()
    if (path.getFileSystem(conf).isFile(path)) {
      singleSequenceFile(path, conf)
    } else {
      multiSequenceFile(path, conf)
    }
  }

  private def singleSequenceFile(path: Path, conf: Configuration): Iterator[Point] = {
    val reader = new Reader(
      conf,
      Reader.file(path))
    require(reader.getKeyClass.equals(classOf[NullWritable]),
      s"Key class should be ${classOf[NullWritable]}")
    require(reader.getValueClass.equals(classOf[BytesWritable]),
      s"Value class should be ${classOf[BytesWritable]}")

    val kryo = getKryo

    new Iterator[Point] {
      var key = NullWritable.get()
      var nextValue = new BytesWritable()
      var value = new BytesWritable()
      var _hasNext = true

      // initialize the iterator
      _hasNext = reader.next(key, nextValue)

      override def hasNext: Boolean = _hasNext

      override def next(): Point = {
        if(!_hasNext)
          throw new NoSuchElementException("No next element")
        value = nextValue
        val toReturn = kryo.readObject(
          new Input(value.copyBytes()),
          classOf[Point])
        _hasNext = reader.next(key, nextValue)
        toReturn
      }
    }
  }

  private def multiSequenceFile(path: Path, conf: Configuration): Iterator[Point] = {
    val fs = path.getFileSystem(conf)
    require(fs.isDirectory(path))
    fs.listStatus(path).iterator.filter(fs => fs.getPath.getName.startsWith("part-")).flatMap { fileStatus =>
      val file = fileStatus.getPath
      singleSequenceFile(file, conf)
    }
  }

  def filename(dir: String, sourceName: String, dim: Int, n: Long, k: Int) =
    s"$dir/$sourceName-$dim-$n-$k.points"

  def saveAsSequenceFile[T:ClassTag](sc: SparkContext, source: PointSource, directory: String): Long = {
    val path = new Path(filename(directory, source.name, source.dim, source.n, source.k))
    val tmpPath = new Path(path.toString + ".tmp")
    val conf = new Configuration()
    
    val meta = Map(
      "data.far-points" -> source.k,
      "data.source" -> source.name,
      "data.dimension" -> source.dim,
      "data.num-points" -> source.n,
      "data.git-revision" -> BuildInfo.gitRevision,
      "data.git-revcount" -> BuildInfo.gitRevCount
    )

    if (path.getFileSystem(conf).exists(path)) {
      throw new IOException(s"File $path already exists")
    }

    val writer = SequenceFile.createWriter(
      conf,
      Writer.file(tmpPath),
      Writer.compression(CompressionType.BLOCK, new DeflateCodec()),
      Writer.keyClass(classOf[NullWritable]),
      Writer.valueClass(classOf[BytesWritable]))

    val kryo = getKryo

    val pl = new ProgressLogger(LoggerFactory.getLogger("serialization") , "points")
    pl.displayFreeMemory = true
    pl.start("Serializing point source....")
    var cnt = 0l
    val key = NullWritable.get()
    val (_, time) = ExperimentUtil.timed {
      for (point <- source.iterator) {
        val bytesRequired = point.data.length*16
        val buf = new Output(bytesRequired)
        kryo.writeObject(buf, point)
        val value = new BytesWritable(buf.toBytes())
        writer.append(key, value)
        pl.update()
        cnt += 1
      }
    }
    pl.stop("Serialization complete!")
    println(s"${ExperimentUtil.convertDuration(time, TimeUnit.MILLISECONDS)} elapsed")

    writer.close()

    writeMetadata(path.toString, meta)

    // FIXME: Should directly generate in parallel
    println("Redistributing as multiple files")
    val sf = sc.sequenceFile(tmpPath.toString, classOf[NullWritable], classOf[BytesWritable], sc.defaultParallelism)
    sf.saveAsSequenceFile(path.toString, Some(classOf[DeflateCodec]))

    tmpPath.getFileSystem(conf).delete(tmpPath, true)

    cnt
  }

  private def metadataName(path: String): String =
    path + ".metadata"

  def metadata(name: String): Map[String, String] = {
    val props = new Properties()
    val path = new Path(metadataName(name))
    val conf = new Configuration()
    val input = path.getFileSystem(conf).open(path)
    props.load(input)
    input.close()

    val pairs: Seq[(String, String)] =
      props.stringPropertyNames().asScala.map { p =>
        (p, props.getProperty(p))
      }.toSeq

    pairs.toMap
  }

  def writeMetadata(fname: String, metaMap: Map[String, Any]): Unit = {
    val props = new Properties()
    for ((k, v) <- metaMap) {
      props.setProperty(k.toString, v.toString)
    }
    val path = new Path(metadataName(fname))
    val conf = new Configuration()
    val out = path.getFileSystem(conf).create(path)
    props.store(out, "")
    out.close()
  }

  def configSerialization(conf: SparkConf) = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "it.unipd.dei.diversity.PointsKryoRegistrator")
  }

  def main(args: Array[String]) = {
    val path = args(0)
    println(s"Metadata for $path")
    println(metadata(path).mkString("\n"))
  }

}

class PointSerializer extends Serializer[Point] {
  def write(kryo: Kryo, output: Output, point: Point) = {
    kryo.writeObject(output, point.data)
  }
  def read(kryo: Kryo, input: Input, clazz: Class[Point]) = {
    val data = kryo.readObject(input, classOf[Array[Double]])
    Point(data)
  }
}

class PointsArraySerializer extends Serializer[Array[Point]] {
  def write(kryo: Kryo, output: Output, points: Array[Point]) = {
    val n = points.length
    val dim = points(0).dimension
    output.writeInt(n)
    output.writeInt(dim)
    var i = 0
    while (i < n) {
      val data = points(i).data
      var j = 0
      while (j < dim) {
        output.writeDouble(data(j))
        j += 1
      }
      i += 1
    }
  }
  def read(kryo: Kryo, input: Input, clazz: Class[Array[Point]]) = {
    val n = input.readInt()
    val dim = input.readInt()
    val points = Array.ofDim[Point](n)
    var i = 0
    while (i < n) {
      val data = Array.ofDim[Double](dim)
      var j = 0
      while (j < dim) {
        data(j) = input.readDouble()
        j += 1
      }
      points(i) = Point(data)
      i += 1
    }
    points
  }
}


class PointsKryoRegistrator extends KryoRegistrator {
  override def registerClasses(k: Kryo) = {
    k.register(classOf[Point], new PointSerializer())
    k.register(classOf[Array[Point]], new PointsArraySerializer())
  }
}
