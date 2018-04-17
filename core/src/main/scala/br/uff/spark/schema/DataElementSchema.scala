package br.uff.spark.schema

trait DataElementSchema[T] extends Serializable {

  def geFieldsNames(): Array[String]

  def getSplitedData(value: T): Array[Array[String]]

}