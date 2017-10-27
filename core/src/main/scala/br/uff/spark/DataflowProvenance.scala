package br.uff.spark

import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.util.UUID

import br.uff.spark.database.{CassandraDBDao, DataBaseBasicMethods, TestDBDao}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object DataflowProvenance {

  private val instance = new DataflowProvenance

  def getInstance: DataflowProvenance = instance
}

class DataflowProvenance private() {

  var dao: DataBaseBasicMethods = null
  var execution: Execution = null
  private val rdds = new mutable.HashMap[String, DataElement[_ <: Any]]()

  def init(sparkContext: SparkContext): Unit = {
    execution = new Execution(sparkContext.appName)
    dao = new CassandraDBDao(execution)
    dao.init()
  }

  initTest()

  // default values
  def initTest(): Unit = {
    execution = new Execution("Teste")
    dao = new TestDBDao(execution)
    dao.init()
  }


  def insertDependencies(dataElement: DataElement[_ <: Any]): Unit = {
    dao.insertDependencies(dataElement)
  }

  def add(task: Task): Unit = {
    println(s"${task.transformationType} - ${task.description} - ${task.id}")
    dao.insertTask(task)
  }

  def add(dataElement: DataElement[_]): Unit = synchronized {
    dao.insertDataElement(dataElement)
  }

  def update(dataElement: DataElement[_]): Unit = synchronized {
    dao.updateDataElement(dataElement)
  }

  def delete(dataElement: DataElement[_]): Unit = synchronized {
    dao.deleteDataElement(dataElement)
  }

  def informNewDepencencie(dataElement: DataElement[_ <: Any], id: UUID): Unit = {
    dao.insertDependencieOfDataElement(dataElement, id)
  }

  def finish(): Unit = {
    execution.endTime = LocalDateTime.now()
    dao.updateExecution(execution)
    dao.close()
  }


  def exportFile(file: File) = {
    var output = new FileWriter(file)
    output.write("{\"nodes\":[\n")
    var first = true
    dao.allFilesOfExecution(execution.ID, (id, value) => {
      if (!first) {
        output.write(",\n")
      } else {
        first = false
      }
      output.write("{\"id\":\"" + id + "\", \"atributos\":{\"value\":\"" + value + "\"}}")
    })
    output.write("], \"links\":[\n")
    first = true
    dao.allRelationshipBetweenDataElement(execution.ID, (source, target) => {
      if (!first) {
        output.write(",\n")
      } else {
        first = false
      }
      output.write("{\"source\": \"" + source + "\", \"target\": \"" + target + "\"}")
    })
    output.write("]}")
    output.flush()
    dao.close()
    output.close()
  }

}