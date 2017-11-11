package br.uff.spark

trait DataElementSchema[T] extends Serializable {

  def geFielsNames(): Array[String]

  def splitData(value: T): Array[String]

}
