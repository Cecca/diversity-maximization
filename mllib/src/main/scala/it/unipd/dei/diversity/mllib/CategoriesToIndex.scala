package it.unipd.dei.diversity.mllib

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class CategoriesToIndex(override val uid: String)
  extends Estimator[CategoriesToIndexModel] with InputCol with OutputCol {

  def this() = this(Identifiable.randomUID("catmap"))

  override def fit(dataset: Dataset[_]): CategoriesToIndexModel = {
    transformSchema(dataset.schema, logging = true)

    val input = dataset.select($(inputCol)).rdd.map(_.getAs[Seq[String]](0))
    val categories = input.flatMap(cats => cats).distinct().collect()
    copyValues(new CategoriesToIndexModel(uid, categories).setParent(this))
  }

  override def copy(extra: ParamMap): Estimator[CategoriesToIndexModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val requiredInType = ArrayType(StringType, containsNull = true)
    val actualDataType = schema($(inputCol)).dataType
    require(actualDataType.equals(requiredInType),
      s"Column ${$(inputCol)} must be of type $requiredInType but was $actualDataType")
    val col = StructField($(outputCol), ArrayType(IntegerType, containsNull = true))
    require(!schema.fieldNames.contains(col.name, s"Column ${col.name} already exists"))
    StructType(schema.fields :+ col)
  }

}

class CategoriesToIndexModel(override val uid: String,
                             private val categories: Array[String])
  extends Model[CategoriesToIndexModel] with MLWritable with InputCol with OutputCol {

  java.util.Arrays.sort(categories.asInstanceOf[Array[AnyRef]])

  def this(categories: Array[String]) = this(Identifiable.randomUID("catmap"), categories)

  // Immutable view of categories
  def getCategories: Seq[String] = categories

  def getCategoriesMap: Map[String, Int] = categories.zipWithIndex.toMap

  override def copy(extra: ParamMap): CategoriesToIndexModel = defaultCopy(extra)

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

  def inverse: IndexToCategories = new IndexToCategories().setIndex(categories)

}

class IndexToCategories(override val uid: String) extends Transformer with InputCol with OutputCol {

  def this() = this(Identifiable.randomUID("catmap"))

  val index: Param[Array[String]] = new Param[Array[String]](this, "index", "category index")

  def getIndex: Array[String] = $(index)

  def setIndex(value: Array[String]): this.type = set(index, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val indexBr = dataset.sparkSession.sparkContext.broadcast(getIndex)
    val mapper = udf { docCats: Seq[Int] =>
      val catIds: Array[String] = indexBr.value
      docCats.map({ c =>
        catIds(c)
      })
    }
    dataset.withColumn($(outputCol), mapper(col($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val requiredInType = ArrayType(IntegerType, containsNull = false)
    val actualDataType = schema($(inputCol)).dataType
    require(actualDataType.equals(requiredInType),
      s"Column ${$(inputCol)} must be of type $requiredInType but was $actualDataType")
    val col = StructField($(outputCol), ArrayType(StringType, containsNull = true))
    require(!schema.fieldNames.contains(col.name, s"Column ${col.name} already exists"))
    StructType(schema.fields :+ col)
  }

}
