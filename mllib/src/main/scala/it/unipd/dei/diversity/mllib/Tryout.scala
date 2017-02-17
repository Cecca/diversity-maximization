package it.unipd.dei.diversity.mllib

import edu.stanford.nlp.simple.Document
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, IDF, StopWordsRemover}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, UnaryTransformer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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

class CategoryMapper(override val uid: String)
  extends Estimator[CategoryMapperModel] {

  def this() = this(Identifiable.randomUID("catmap"))

  // Input and output columns
  val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  def getInputCol: String = $(inputCol)

  def setInputCol(value: String): this.type = set(inputCol, value)

  val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  def getOutputCol: String = $(outputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): CategoryMapperModel = {
    transformSchema(dataset.schema, logging = true)

    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Seq[String]](0))
    val categories = input.flatMap(cats => cats).distinct().collect()
    copyValues(new CategoryMapperModel(uid, categories).setParent(this))
  }

  override def copy(extra: ParamMap): Estimator[CategoryMapperModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val requiredInType = ArrayType(StringType, containsNull = true)
    val actualDataType = schema($(inputCol)).dataType
    require(actualDataType.equals(requiredInType),
      s"Column ${$(inputCol)} must be of type $requiredInType but was $actualDataType")
    val col = StructField($(outputCol), ArrayType(StringType, containsNull = true))
    require(!schema.fieldNames.contains(col.name, s"Column ${col.name} already exists"))
    StructType(schema.fields :+ col)
  }

}

class CategoryMapperModel(override val uid: String,
                          private val categories: Array[String])
  extends Model[CategoryMapperModel] with MLWritable {

  java.util.Arrays.sort(categories.asInstanceOf[Array[AnyRef]])

  def this(categories: Array[String]) = this(Identifiable.randomUID("catmap"), categories)

  // Input and output columns
  val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  def getInputCol: String = $(inputCol)

  def setInputCol(value: String): this.type = set(inputCol, value)

  val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  def getOutputCol: String = $(outputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  // Immutable view of categories
  def getCategories: Seq[String] = categories

  def getCategoriesMap: Map[String, Int] = categories.zipWithIndex.toMap

  override def copy(extra: ParamMap): CategoryMapperModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val catBr = dataset.sparkSession.sparkContext.broadcast(categories)
    val mapper = udf { docCats: Seq[String] =>
      val catIds: Array[AnyRef] = catBr.value.asInstanceOf[Array[AnyRef]]
      docCats.map({ c =>
        val idx = java.util.Arrays.binarySearch(catIds, c.asInstanceOf[AnyRef])
        require(idx >= 0, s"Unknown category $c")
        idx
      })
    }
    dataset.withColumn($(outputCol), mapper(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val requiredInType = ArrayType(StringType, containsNull = true)
    val actualDataType = schema($(inputCol)).dataType
    require(actualDataType.equals(requiredInType),
      s"Column ${$(inputCol)} must be of type $requiredInType but was $actualDataType")
    val col = StructField($(outputCol), ArrayType(StringType, containsNull = true))
    require(!schema.fieldNames.contains(col.name, s"Column ${col.name} already exists"))
    StructType(schema.fields :+ col)
  }

  override def write: MLWriter = ???
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

    val categories = new CategoryMapper()
      .setInputCol("categories")
      .setOutputCol("cats")
      .fit(vecs)
    //    println(s"Categories are:\n${categories.getCategories.mkString("\n")}")
    val catVecs = categories.transform(vecs).drop("categories")

    catVecs.explain(true)
    catVecs.show()
  }
}
