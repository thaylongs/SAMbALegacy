package br.uff.spark.database

import java.util.UUID

import br.uff.spark.{DataElement, Execution, Task}

trait DataBaseBasicMethods {
  def insertTask(task: Task): Unit = {}

  def close(): Unit = {}

  def updateExecution(execution: Execution): Unit = {}

  def init(): Unit = {}

  def insertDataElement(dataElement: DataElement[_ <: Any]): Unit = {}

  def insertDependencies(dataElement: DataElement[_ <: Any]): Unit = {}

  def insertDependencieOfDataElement(dataElement: DataElement[_ <: Any], id: UUID): Unit = {}

  def updateDataElement(dataElement: DataElement[_ <: Any]): Unit = {}

  def deleteDataElement(dataElement: DataElement[_ <: Any]): Unit = {}

  def allFilesOfExecution(id: UUID, onRead: (String, String) => Unit): Unit = {}

  def allRelationshipBetweenDataElement(id: UUID, onRead: (String, String) => Unit): Unit = {}
}
