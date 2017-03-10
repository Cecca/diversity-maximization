package it.unipd.dei.diversity.mllib

import org.apache.spark.sql.types.{DataType, StructField, StructType}

object SchemaUtils {

  def checkColumnTypes(
                        schema: StructType,
                        colName: String,
                        dataTypes: Seq[DataType],
                        msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(dataTypes.exists(actualDataType.equals),
      s"Column $colName must be of type equal to one of the following types: " +
        s"${dataTypes.mkString("[", ", ", "]")} but was actually of type $actualDataType.$message")
  }

  def appendColumn(
                    schema: StructType,
                    colName: String,
                    dataType: DataType,
                    nullable: Boolean = false): StructType = {
    if (colName.isEmpty) return schema
    appendColumn(schema, StructField(colName, dataType, nullable))
  }

  def appendColumn(schema: StructType, col: StructField): StructType = {
    require(!schema.fieldNames.contains(col.name), s"Column ${col.name} already exists.")
    StructType(schema.fields :+ col)
  }

}
