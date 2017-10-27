package br.uff.spark

import java.io.Serializable
import java.util.UUID

import br.uff.spark.TransformationType.TransformationType
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Task(@transient val rdd: RDD[_ <: Any]) extends Serializable {

  val id = UUID.randomUUID()
  var transformationType: TransformationType = TransformationType.UNKNOWN
  var isIgnored = false
  var alreadyPersisted = false
  var description: String = ""

  val dependenciesIDS = new mutable.MutableList[UUID]()

  def addDepencencie(rdd: RDD[_ <: Any]): Task = addDepencencie(rdd.task)

  def addDepencencie(task: Task): Task = {
    if (task.isIgnored) {
      for (elem <- task.dependenciesIDS) {
        dependenciesIDS += elem
      }
    } else {
      dependenciesIDS += task.id
    }
    this
  }

  def checkAndPersist(): Unit = synchronized {
    if (!alreadyPersisted && !isIgnored) {
      DataflowProvenance.getInstance.add(this)
    }
    alreadyPersisted = true
  }

  //  def addDepencencie(taskID: String): Task = synchronized {
  //    if (taskID == null)
  //      throw new Exception("Errror ----- d")
  //    this.dependenciesIDS += taskID
  //    return this
  //  }

  override def toString = s"Task($id, $transformationType, $isIgnored)"
}
