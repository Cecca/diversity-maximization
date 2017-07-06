package it.unipd.dei.diversity.mllib

import edu.stanford.nlp.simple.Document
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.JavaConversions._


/**
  * Lemmatizer that uses Stanford-NLP library
  */
class Lemmatizer(override val uid: String)
  extends UnaryTransformer[String, Seq[String], Lemmatizer] with DefaultParamsWritable {

  val symbols = "^[',\\.`/-_\\+]+$".r

  val specialTokens = Set(
    "-lsb-", "-rsb-", "-lrb-", "-rrb-", "'s", "--"
  )

  def this() = this(Identifiable.randomUID("lem"))

  override protected def createTransformFunc: String => Seq[String] = { str: String =>
    val doc = new Document(str.toLowerCase())
    doc.sentences.flatMap { sentence =>
      sentence.lemmas()
        .filterNot(lem => symbols.findFirstIn(lem).isDefined) // Remove tokens made by symbols only
        .filterNot(specialTokens.contains)
    }
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): Lemmatizer = defaultCopy(extra)
}

object Lemmatizer extends DefaultParamsReadable[Lemmatizer] {

  override def load(path: String): Lemmatizer = super.load(path)
}