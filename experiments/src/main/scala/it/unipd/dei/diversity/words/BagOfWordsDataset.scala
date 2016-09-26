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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }
import java.io.IOException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{ CompressionType, Writer }
import org.apache.hadoop.io.compress.DeflateCodec
import org.apache.hadoop.io.{ BytesWritable, NullWritable, SequenceFile }
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.SequenceFileRDDFunctions
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf
import scala.reflect.ClassTag

class BagOfWordsDataset(val seqFile: String) {

  def documents(sc: SparkContext, parallelism: Int): RDD[DocumentBagOfWords] = {
    val bytes = sc.sequenceFile(seqFile, classOf[NullWritable], classOf[BytesWritable], parallelism)
    bytes.map { case (_, bytesArr) =>
      BagOfWordsDataset.getKryo.readObject(new Input(bytesArr.copyBytes()), classOf[DocumentBagOfWords])
    }
  }

  def documents(): Iterator[DocumentBagOfWords] = {
    val path = new Path(seqFile)
    val conf = new Configuration()
    if (path.getFileSystem(conf).isFile(path)) {
      singleSequenceFile(path, conf)
    } else {
      multiSequenceFile(path, conf)
    }
  }

  private def singleSequenceFile(path: Path, conf: Configuration): Iterator[DocumentBagOfWords] = {
    val reader = new Reader(
      conf,
      Reader.file(path))
    require(reader.getKeyClass.equals(classOf[NullWritable]),
      s"Key class should be ${classOf[NullWritable]}")
    require(reader.getValueClass.equals(classOf[BytesWritable]),
      s"Value class should be ${classOf[BytesWritable]}")

    val kryo = BagOfWordsDataset.getKryo

    new Iterator[DocumentBagOfWords] {
      var key = NullWritable.get()
      var nextValue = new BytesWritable()
      var value = new BytesWritable()
      var _hasNext = true

      // initialize the iterator
      _hasNext = reader.next(key, nextValue)

      override def hasNext: Boolean = _hasNext

      override def next(): DocumentBagOfWords = {
        if(!_hasNext)
          throw new NoSuchElementException("No next element")
        value = nextValue
        val toReturn = kryo.readObject(
          new Input(value.copyBytes()),
          classOf[DocumentBagOfWords])
        _hasNext = reader.next(key, nextValue)
        toReturn
      }
    }
  }

  private def multiSequenceFile(path: Path, conf: Configuration): Iterator[DocumentBagOfWords] = {
    val fs = path.getFileSystem(conf)
    require(fs.isDirectory(path))
    fs.listStatus(path).iterator.filter(fs => fs.getPath.getName.startsWith("part-")).flatMap { fileStatus =>
      val file = fileStatus.getPath
      singleSequenceFile(file, conf)
    }
  }

}

object BagOfWordsDataset {

  def getKryo: Kryo = {
    val _kryo = new Kryo()
    _kryo.register(classOf[DocumentBagOfWords], new BOWKryoSerializer())
    _kryo
  }

  def saveAsSequenceFile[T:ClassTag](docs: RDD[DocumentBagOfWords], filename: String) = {
    val path = new Path(filename)
    val conf = new Configuration()

    if (path.getFileSystem(conf).exists(path)) {
      throw new IOException(s"File $path already exists")
    }

    docs.map { doc =>
      val kryo = BagOfWordsDataset.getKryo
      val buf = new Output(16384)
      kryo.writeObject(buf, doc)
      val value = new BytesWritable(buf.toBytes())
      val key = NullWritable.get()
      (key, value)
    }.saveAsSequenceFile(filename, Some(classOf[DeflateCodec]))
  }

  def main(args: Array[String]) = {
    val opts = new Opts(args)
    opts.verify()

    val sConf = new SparkConf(true).setAppName("Bag of Words dataset conversion")
    val sc = new SparkContext(sConf)

    val documents = opts.format() match {
      case "mxm" => new MXMBagOfWordsDataset(opts.input()).documents(sc)
      case "uci" =>
        UCIBagOfWordsDataset.fromName(opts.input(), opts.directory()).documents(sc)
    }

    saveAsSequenceFile(documents, opts.output())

  }

  class Opts(args: Array[String]) extends ScallopConf(args) {

    lazy val format = opt[String](required=true, descr="Possible formats: mxm, uci")

    lazy val input = opt[String](required=true, descr="Input path")

    lazy val directory = opt[String](required=false, descr="Directory for UCI datasets")

    lazy val output = opt[String](required=true, descr="Output path")

  }

}
