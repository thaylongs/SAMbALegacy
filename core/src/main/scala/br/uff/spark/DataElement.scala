package br.uff.spark

import java.io.Serializable
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.beans.BeanProperty
import scala.collection.mutable

object DataElement {


  def dummy[T](element: T): DataElement[T] = {
    val result = new DataElement[T](element, null, true)
    result.dependenciesIDS = null
    result.id = null
    result
  }

  def of[T](element: T): DataElement[T] = of(element, null, false)

  def of[T](element: T, task: Task, ignore: Boolean): DataElement[T] = {
    val result = new DataElement(element, task, ignore)
    if (!ignore)
      result.persist
    result
  }

  def of[T, Z](element: T, task: Task, ignore: Boolean, dependencies: java.util.List[DataElement[Z]]): DataElement[T] =
    of(element, task, ignore, dependencies.asScala: _*)

  def of[T](element: T, task: Task, ignore: Boolean, dependencies: DataElement[_ <: Any]*): DataElement[T] = {
    val result = new DataElement(element, task, ignore)
    for (dependencie <- dependencies) {
      if (dependencie.ignore) {
        for (dependencieID <- dependencie.dependenciesIDS) {
          result.dependenciesIDS += dependencieID
        }
      } else {
        result.dependenciesIDS += dependencie.id
      }
    }
    if (!ignore)
      result.persist
    result
  }

  def of[T](element: T, task: Task, ignore: Boolean, dependencies: mutable.MutableList[UUID]): DataElement[T] = {
    val result = new DataElement(element, task, ignore)
    result.dependenciesIDS ++= dependencies
    if (!ignore)
      result.persist
    result
  }

  def ignoringSchemaOf[T](element: T, task: Task, ignore: Boolean, dependencies: DataElement[_ <: Any]*): DataElement[T] = {
    val result = new DataElement(element, task, ignore) {
      override def applySchemaToTheValue(): util.List[String] = {
        return java.util.Arrays.asList(this.value.toString)
      }
    }

    for (dependencie <- dependencies) {
      if (dependencie.ignore) {
        for (dependencieID <- dependencie.dependenciesIDS) {
          result.dependenciesIDS += dependencieID
        }
      } else {
        result.dependenciesIDS += dependencie.id
      }
    }
    if (!ignore)
      result.persist
    result
  }

}

class DataElement[T](var value: T, var task: Task, var ignore: Boolean = false) extends Serializable {

  @BeanProperty
  var id: UUID = UUID.randomUUID()

  var dependenciesIDS = new mutable.MutableList[UUID]()

  def persist: DataElement[T] = {
    DataflowProvenance.getInstance.add(this)
    this
  }

  def addDepencencie(dataElementsIDs: Iterator[UUID]): DataElement[T] = {
    for (elem <- dataElementsIDs) {
      this.dependenciesIDS += elem
      if (!ignore)
        DataflowProvenance.getInstance.informNewDependency(this, elem)
    }
    return this
  }

  def addDepencencie(dependencie: DataElement[_ <: Any]): DataElement[T] = synchronized {
    if (dependencie == null)
      throw new NullPointerException("Error: the dependencies are null")
    if (dependencie.ignore) {
      for (dependencieID <- dependencie.dependenciesIDS) {
        this.dependenciesIDS += dependencieID
        if (!ignore)
          DataflowProvenance.getInstance.informNewDependency(this, dependencieID)
      }
    } else {
      this.dependenciesIDS += dependencie.id
      if (!ignore)
        DataflowProvenance.getInstance.informNewDependency(this, dependencie.id)
    }
    return this
  }

  def addDepencencie(dependencies: mutable.MutableList[UUID]): DataElement[T] = synchronized {
    if (dependencies == null)
      throw new NullPointerException("Error: the dependencies are null")
    if (this.ignore) {
      throw new Exception("This method can't be called when it needs to be ignored")
    }
    this.dependenciesIDS = dependencies
    DataflowProvenance.getInstance.insertDependencies(this)
    return this
  }

  def updateValue(value: Any): DataElement[T] = {
    this.value = value.asInstanceOf[T]
    if (!ignore) {
      DataflowProvenance.getInstance.update(this)
    }
    this
  }

  def applySchemaToTheValue(): java.util.List[String] = {
    val result =
      if (task == null || task.schema == null)
        Array(value.toString)
      else
        task.parseValue.apply(value)
    return java.util.Arrays.asList(result: _*)
  }

  def cloneWithNewValue[W](newValue: W): DataElement[W] = {
    val newDE = new DataElement(newValue, task, ignore)
    newDE.id = id
    newDE.dependenciesIDS = dependenciesIDS
    newDE
  }

  def deleteIt(): Unit = {
    DataflowProvenance.getInstance.delete(this)
  }

  override def toString = s"DataElement($id, $value, $task, $ignore)"
}
