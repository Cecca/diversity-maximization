package it.unipd.dei.diversity

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{Reader, Writer}
import org.apache.hadoop.io.{BytesWritable, NullWritable, SequenceFile}

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

  def saveAsSequenceFile[T:ClassTag](iterator: Iterator[T], file: String): Long = {
    val path = new Path(file)
    val conf = new Configuration()

    if (path.getFileSystem(conf).exists(path)) {
      throw new IOException(s"File $path already exists")
    }

    val writer = SequenceFile.createWriter(
      conf,
      Writer.file(path),
      Writer.keyClass(classOf[NullWritable]),
      Writer.valueClass(classOf[BytesWritable]))

    var cnt = 0
    val key = NullWritable.get()
    for (v <- iterator) {
      // The value must be wrapped in a Array because of how the values are
      // deserialized by Spark
      val value = new BytesWritable(serialize(Array(v)))
      writer.append(key, value)
      cnt += 1
    }

    writer.close()
    cnt
  }

}
