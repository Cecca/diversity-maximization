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
import java.util.concurrent.TimeUnit

import it.unipd.dei.diversity.source.PointSource
import it.unimi.dsi.logging.ProgressLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile.{Reader, Writer}
import org.apache.hadoop.io.{BytesWritable, NullWritable, SequenceFile}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object SerializationUtils {

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
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

    val blocks = new Iterator[Array[Point]] {
      var key = NullWritable.get()
      var nextValue = new BytesWritable()
      var value = new BytesWritable()
      var _hasNext = true

      // initialize the iterator
      _hasNext = reader.next(key, nextValue)

      override def hasNext: Boolean = _hasNext

      override def next(): Array[Point] = {
        if(!_hasNext)
          throw new NoSuchElementException("No next element")
        value = nextValue
        val toReturn = deserialize[Array[Point]](value.copyBytes())
        _hasNext = reader.next(key, nextValue)
        toReturn
      }
    }

    // Flatten the blocks
    blocks.flatMap(_.iterator)
  }

  private def multiSequenceFile(path: Path, conf: Configuration): Iterator[Point] = {
    val fs = path.getFileSystem(conf)
    require(fs.isDirectory(path))
    fs.listStatus(path).iterator.filter(fs => fs.getPath.getName.startsWith("part-")).flatMap { fileStatus =>
      val file = fileStatus.getPath
      singleSequenceFile(file, conf)
    }
  }

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  def filename(dir: String, sourceName: String, dim: Int, n: Int, k: Int) =
    s"$dir/$sourceName-$dim-$n-$k.points"

  def saveAsSequenceFile[T:ClassTag](source: PointSource, directory: String): Long = {
    val path = new Path(filename(directory, source.name, source.dim, source.n, source.k))
    val conf = new Configuration()
    val meta = new SequenceFile.Metadata()
    meta.set(new Text("far-points"), new Text(source.k.toString))
    meta.set(new Text("source-name"), new Text(source.name))
    meta.set(new Text("dimension"), new Text(source.dim.toString))
    meta.set(new Text("num-points"), new Text(source.n.toString))

    if (path.getFileSystem(conf).exists(path)) {
      throw new IOException(s"File $path already exists")
    }

    val writer = SequenceFile.createWriter(
      conf,
      Writer.file(path),
      Writer.metadata(meta),
      Writer.keyClass(classOf[NullWritable]),
      Writer.valueClass(classOf[BytesWritable]))

    var cnt = 0l
    val key = NullWritable.get()
    // The value must be wrapped in a Array because of how the values are
    // deserialized by Spark. We wrap more points in a single array for efficiency
    val (_, time) = ExperimentUtil.timed {
      for (vs <- source.iterator.grouped(16384)) {
        val varr = vs.toArray
        val value = new BytesWritable(serialize(varr))
        writer.append(key, value)
        cnt += varr.length
        println(s"--> $cnt items")
      }
    }
    println(s"${ExperimentUtil.convertDuration(time, TimeUnit.MILLISECONDS)} elapsed")

    writer.close()
    cnt
  }

  // def saveAsSequenceFile[T:ClassTag](iterator: Iterator[T], file: String): Long = {
  //   val path = new Path(file)
  //   val conf = new Configuration()

  //   if (path.getFileSystem(conf).exists(path)) {
  //     throw new IOException(s"File $path already exists")
  //   }

  //   val writer = SequenceFile.createWriter(
  //     conf,
  //     Writer.file(path),
  //     Writer.keyClass(classOf[NullWritable]),
  //     Writer.valueClass(classOf[BytesWritable]))

  //   var cnt = 0
  //   val key = NullWritable.get()
  //   // The value must be wrapped in a Array because of how the values are
  //   // deserialized by Spark. We wrap more points in a single array for efficiency
  //   val (_, time) = ExperimentUtil.timed {
  //     for (vs <- iterator.grouped(16384)) {
  //       val varr = vs.toArray
  //       val value = new BytesWritable(serialize(varr))
  //       writer.append(key, value)
  //       cnt += varr.length
  //       println(s"--> $cnt items")
  //     }
  //   }
  //   println(s"${ExperimentUtil.convertDuration(time, TimeUnit.MILLISECONDS)} elapsed")

  //   writer.close()
  //   cnt
  // }

}
