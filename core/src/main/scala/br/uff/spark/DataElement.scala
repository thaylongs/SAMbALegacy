package br.uff.spark

import java.io.Serializable
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.{Random, UUID}

import com.fasterxml.uuid.Generators

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

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
    for (dependency <- dependencies) {
      if (dependency.ignore) {
        result.dependenciesIDS.addAll(dependency.dependenciesIDS)
      } else {
        result.dependenciesIDS.add(dependency.id)
      }
    }
    if (!ignore)
      result.persist
    result
  }

  def of[T](element: T, task: Task, ignore: Boolean, dependencies: java.util.ArrayList[UUID]): DataElement[T] = {
    val result = new DataElement(element, task, ignore)
    result.dependenciesIDS.addAll(dependencies)
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

    for (dependency <- dependencies) {
      if (dependency.ignore) {
        result.dependenciesIDS.addAll(dependency.dependenciesIDS)
      } else {
        result.dependenciesIDS.add(dependency.id)
      }
    }

    if (!ignore)
      result.persist
    result
  }

  var UUID_Gen = Generators.randomBasedGenerator(new Random(512))

}

class DataElement[T](var value: T, var task: Task, var ignore: Boolean = false) extends Serializable {

  @BeanProperty
  var id: UUID =
    if (ignore) null
    else DataElement.UUID_Gen.generate()

  var dependenciesIDS: java.util.List[UUID] = new java.util.ArrayList[UUID]()

  def persist: DataElement[T] = {
    DataflowProvenance.getInstance.add(this)
    this
  }

  def addDependencies(dataElementsIDs: java.util.List[UUID]): DataElement[T] = {
    this.dependenciesIDS.addAll(dataElementsIDs)
    if (!ignore) {
      DataflowProvenance.getInstance.informNewDependencies(this, dataElementsIDs)
    }
    return this
  }

  def addDependency(dependency: DataElement[_ <: Any]): DataElement[T] = {
    if (dependency == null)
      throw new NullPointerException("Error: the dependency is null")
    if (dependency.ignore) {
      this.dependenciesIDS.addAll(dependency.dependenciesIDS)
      if (!ignore) {
        DataflowProvenance.getInstance.informNewDependencies(this, dependency.dependenciesIDS)
      }
    } else {
      this.dependenciesIDS.add(dependency.id)
      if (!ignore)
        DataflowProvenance.getInstance.informNewDependency(this, dependency.id)
    }
    return this
  }

  def setDependencies(dependencies: java.util.List[UUID]): DataElement[T] = {
    if (dependencies == null)
      throw new NullPointerException("Error: the dependencies are null")
    if (this.ignore) {
      throw new Exception("This method can't be called when it needs to be ignored")
    }
    this.dependenciesIDS = dependencies
    DataflowProvenance.getInstance.setDependencies(this)
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
