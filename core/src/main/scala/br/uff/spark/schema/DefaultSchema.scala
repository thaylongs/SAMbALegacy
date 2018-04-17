package br.uff.spark.schema

class DefaultSchema[T <: Any] extends DataElementSchema[T] {

  override def geFieldsNames(): Array[String] = Array("Value")

  override def getSplitedData(value: T) = Array(Array(value.toString))

}
