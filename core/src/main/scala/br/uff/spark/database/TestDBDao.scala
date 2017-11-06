package br.uff.spark.database
import java.util.UUID

import br.uff.spark.{DataElement, Execution, Task}

class TestDBDao(val execution: Execution) extends DataBaseBasicMethods {

  override def insertTask(task: Task): Unit = super.insertTask(task)

  override def close(): Unit = super.close()

  override def updateExecution(execution: Execution): Unit = super.updateExecution(execution)

  override def init(): Unit = super.init()

  override def insertDataElement(dataElement: DataElement[_]): Unit = super.insertDataElement(dataElement)

  override def insertDependencies(dataElement: DataElement[_]): Unit = super.insertDependencies(dataElement)

  override def insertDependencyOfDataElement(dataElement: DataElement[_], id: UUID): Unit = super.insertDependencyOfDataElement(dataElement, id)

  override def updateDataElement(dataElement: DataElement[_]): Unit = super.updateDataElement(dataElement)

  override def deleteDataElement(dataElement: DataElement[_]): Unit = super.deleteDataElement(dataElement)

  override def allFilesOfExecution(id: UUID, onRead: (String, String) => Unit): Unit = super.allFilesOfExecution(id, onRead)

  override def allRelationshipBetweenDataElement(id: UUID, onRead: (String, String) => Unit): Unit = super.allRelationshipBetweenDataElement(id, onRead)
}
