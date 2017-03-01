package it.unipd.dei.diversity.mllib

import org.apache.spark.ml.param.{Param, Params}

/**
  * Modeled on HasInputCol
  */
trait InputCol extends Params {

  val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  def getInputCol: String = $(inputCol)

  def setInputCol(value: String): this.type = set(inputCol, value)

}

trait OutputCol extends Params {

  val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  def getOutputCol: String = $(outputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}