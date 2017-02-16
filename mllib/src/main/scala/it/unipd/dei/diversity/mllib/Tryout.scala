package it.unipd.dei.diversity.mllib

import edu.stanford.nlp.simple.Document
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.feature.{CountVectorizer, IDF, StopWordsRemover}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model, UnaryTransformer}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.JavaConversions._

case class CategorizedBow(id: Long,
                          categories: Array[String],
                          title: String,
                          counts: Vector)

/**
  * Lemmatizer that uses Stanford-NLP library
  */
class Lemmatizer(override val uid: String)
  extends UnaryTransformer[String, Seq[String], Lemmatizer] with DefaultParamsWritable {

  val symbols = "^[',\\.`/-_]+$".r

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

class CategoryMapperModel(override val uid: String)
  extends Model[CategoryMapperModel] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("catmap"))

  override def copy(extra: ParamMap): CategoryMapperModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = ???

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

}

object Tryout {

  def textToWords(dataset: DataFrame): DataFrame = {
    val lemmatizer = new Lemmatizer()
      .setInputCol("text")
      .setOutputCol("lemmas")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("lemmas")
      .setOutputCol("words")
      .setCaseSensitive(false)
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
    stopWordsRemover.transform(lemmatizer.transform(dataset)).drop("lemmas")
  }

  def wordsToVec(dataset: DataFrame): DataFrame = {
    val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("tmp-counts")
      .setVocabSize(5000) // Restrict to the 5000 most-frequent words
      .fit(dataset)
    val counts = countVectorizer.transform(dataset)

    val idf = new IDF()
      .setInputCol("tmp-counts")
      .setOutputCol("tf-idf")
      .fit(counts)
    idf.transform(counts).drop("tmp-counts")
  }

  def main(args: Array[String]) {

    val path = "/data/matteo/Wikipedia/enwiki-20170120/AA/wiki_00.bz2"

    val conf = new SparkConf(loadDefaults = true)
    val spark = SparkSession.builder()
      .appName("tryout")
      .master("local")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val raw = spark
      .read.json(path)
      .select("id", "categories", "title", "text")

    val words = textToWords(raw)
    val vecs = wordsToVec(words)
      .withColumnRenamed("tf-idf", "counts")
      .select("id", "title", "categories", "counts")
      .as[CategorizedBow]

    vecs.printSchema()
    vecs.explain()

    vecs.write.parquet("vectors.pq")
  }
}
