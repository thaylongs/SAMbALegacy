package br.uff.spark

import java.io.Serializable
import java.util.UUID

import scala.beans.BeanProperty
import scala.collection.mutable

object DataElement {

  def dummy[W](element: W): DataElement[W] = {
    val result = new DataElement[W](element, null, true)
    result.dependenciesIDS=null
    result.id=null
//    result.stringValue=null
    result
  }

  def of[W](element: W): DataElement[W] = of(element, null, false)

  def of[W](element: W, task: Task, ignore: Boolean): DataElement[W] = {
    val result = new DataElement(element, task, ignore)
    if (!ignore)
      result.persist
    result
  }

  def of[W](element: W, dependencie: UUID, task: Task): DataElement[W] = of(element, dependencie, task, false)

  def of[W](element: W, dependencie: UUID, task: Task, ignore: Boolean): DataElement[W] = {
    val result = new DataElement(element, task, ignore)
    result.dependenciesIDS += dependencie
    if (!ignore)
      result.persist
    else
      throw new Exception("Houston we have a problem - um objeto ignorado foi add ")
    result
  }

  def of[W](element: W, dependencie: DataElement[_ <: Any], task: Task, ignore: Boolean = false): DataElement[W] = {
    val result = new DataElement(element, task, ignore)
    if (dependencie.ignore) {
      for (dependencieID <- dependencie.dependenciesIDS) {
        result.dependenciesIDS += dependencieID
      }
    } else {
      result.dependenciesIDS += dependencie.id
    }
    if (!ignore)
      result.persist
    result
  }

  def of[W](element: W, task: Task, ignore: Boolean, dependencies: DataElement[_ <: Any]*): DataElement[W] = {
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

  def of[W](element: W, task: Task, ignore: Boolean, dependencies: mutable.MutableList[UUID]): DataElement[W] = {
    val result = new DataElement(element, task, ignore)
    result.dependenciesIDS ++= dependencies
    if (!ignore)
      result.persist
    result
  }


}

class DataElement[T](var value: T, var task: Task, var ignore: Boolean = false) extends Serializable {

  @BeanProperty
  var id: UUID = UUID.randomUUID()

//  @BeanProperty
//  var stringValue: String = if (ignore || value == null) null else value.toString

  var dependenciesIDS = new mutable.MutableList[UUID]()

  def persist: DataElement[T] = {
    DataflowProvenance.getInstance.add(this)
    this
  }

  def addDepencencie(dataElementsIDs: Iterator[UUID]): DataElement[T] = {
    for (elem <- dataElementsIDs) {
      this.dependenciesIDS += elem
      if (!ignore)
        DataflowProvenance.getInstance.informNewDepencencie(this, elem)
    }
    return this
  }

  def addDepencencie(dependencie: DataElement[_ <: Any]): DataElement[T] = synchronized {
    if (dependencie == null)
      throw new Exception("Errror ----- d")
    if (dependencie.ignore) {
      for (dependencieID <- dependencie.dependenciesIDS) {
        this.dependenciesIDS += dependencieID
        if (!ignore)
          DataflowProvenance.getInstance.informNewDepencencie(this, dependencieID)
      }
    } else {
      this.dependenciesIDS += dependencie.id
      if (!ignore)
        DataflowProvenance.getInstance.informNewDepencencie(this, dependencie.id)
    }
    return this
  }

  def addDepencencie(dependencies: mutable.MutableList[UUID]): DataElement[T] = synchronized {
    if (dependencies == null)
      throw new Exception("Errror ----- d")
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
//      this.stringValue = value.toString
      DataflowProvenance.getInstance.update(this)
    }
    this
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
