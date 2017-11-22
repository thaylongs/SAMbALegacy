package br.uff.spark

trait DataElementSchema[T] extends Serializable {

  def geFieldsNames(): Array[String]

  def splitData(value: T): Array[String]

}