package br.uff.spark.database

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID

import br.uff.spark.{DataElement, DataSource, Execution, Task}

import scala.collection.JavaConverters._

class CassandraDBDao(val execution: Execution) extends DataBaseBasicMethods {

  val con = DataSource.getConnection

  private val STMT_INSERT_TASK = con.prepare(
    """
      |INSERT INTO dfanalyzer."Task"
      |            ("executionID", id, description, "transformationType")
      |VALUES
      |            (?, ?, ?, ?);
    """.stripMargin)

  private val STMT_DEPENDENCIES_OF_TASK = con.prepare(
    """
      |INSERT INTO dfanalyzer."DependenciesOfTask"
      |            ("executionID", target, "source")
      |VALUES
      |            (?, ?, ?);
    """.stripMargin)

  override def insertTask(task: Task): Unit = {

    con.executeAsync(
      STMT_INSERT_TASK.bind(
        execution.ID,
        task.id,
        task.description,
        task.transformationType.toString
      )
    )

    con.executeAsync(
      STMT_DEPENDENCIES_OF_TASK.bind(
        execution.ID,
        task.id,
        task.dependenciesIDS.toSet.asJava
      )
    )
  }


  override def close(): Unit = {
    DataSource.close()
  }

  val STMT_UPDATE_EXECUTION = con.prepare(
    """
      |UPDATE dfanalyzer."Execution"
      |SET    "EndTime" = ?
      |WHERE  id = ?;
    """.stripMargin)

  override def updateExecution(execution: Execution): Unit = {
    con.executeAsync(
      STMT_UPDATE_EXECUTION.bind(
        Timestamp.from(execution.endTime.toInstant(ZonedDateTime.now(ZoneId.systemDefault()).getOffset)),
        execution.ID
      )
    )

  }

  val STMT_INSERT_EXECUTION = con.prepare(
    """
      | INSERT INTO dfanalyzer."Execution"
      |            (id, "StartTime", "EndTime", "AplicationName")
      |VALUES
      |            (?, ?, ?, ?);
    """.stripMargin)

  override def init(): Unit = {
    con.executeAsync(
      STMT_INSERT_EXECUTION.bind(
        execution.ID,
        Timestamp.from(execution.startTime.toInstant(ZonedDateTime.now(ZoneId.systemDefault()).getOffset)),
        null,
        execution.name
      )
    )
  }

  val STMT_INSERT_DATA_ELEMENT = con.prepare(
    """
      |INSERT INTO dfanalyzer."DataElement"
      |            (id, value, "executionID")
      |VALUES
      |            (?, ?, ?);
    """.stripMargin)

  override def insertDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_INSERT_DATA_ELEMENT.bind(
        dataElement.id,
        dataElement.value.toString,
        execution.ID
      )
    )

    if (!dataElement.dependenciesIDS.isEmpty)
      insertDependencies(dataElement)
  }

  val STMT_INSERT_DEPENDENCIES_OF_DATA_ELEMENT = con.prepare(
    """
      | INSERT INTO dfanalyzer."DependenciesOfDataElement"
      |            (source, target, task, "executionID")
      |VALUES
      |            (?, ?, ?, ?);
    """.stripMargin)

  override def insertDependencies(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_INSERT_DEPENDENCIES_OF_DATA_ELEMENT.bind(
        dataElement.dependenciesIDS.toSet.asJava,
        dataElement.id,
        dataElement.task.id,
        execution.ID
      )
    )
  }

  val STMT_UPDATE_DEPENDENCIES_OF_DATA_ELEMENT = con.prepare(
    """
      |UPDATE dfanalyzer."DependenciesOfDataElement"
      |SET    source = source + ?
      |WHERE  target = ?
      |       AND task = ?
      |       AND "executionID" = ?
    """.stripMargin)

  override def insertDependencieOfDataElement(dataElement: DataElement[_ <: Any], id: UUID): Unit = {
    con.executeAsync(
      STMT_UPDATE_DEPENDENCIES_OF_DATA_ELEMENT.bind(
        Set(id).asJava,
        dataElement.id,
        dataElement.task.id,
        execution.ID
      )
    )
  }


  val STMT_UPDATE_DATA_ELEMENT = con.prepare(
    """
      |UPDATE
      |   dfanalyzer."DataElement"
      |SET
      |   value = ?
      |WHERE
      |     "executionID"=?
      |  AND
      |     id=?;
    """.stripMargin)

  override def updateDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_UPDATE_DATA_ELEMENT.bind(
        dataElement.value.toString,
        execution.ID,
        dataElement.id
      )
    )
  }

  val STMT_DELETE_DEPENDENCIES_OF_DATA_ELEMENT = con.prepare(
    """
      |DELETE
      |FROM dfanalyzer."DependenciesOfDataElement"
      |WHERE
      |       "executionID"=?
      |  AND
      |       task=?
      |  AND
      |       target=?
    """.stripMargin)

  val STMT_DELETE_DATA_ELEMENT = con.prepare(
    """
      |DELETE
      |FROM dfanalyzer."DataElement"
      |WHERE
      |       "executionID"=?
      |  AND
      |       id=?
    """.stripMargin)

  override def deleteDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    con.executeAsync(
      STMT_DELETE_DEPENDENCIES_OF_DATA_ELEMENT.bind(
        execution.ID,
        dataElement.task.id,
        dataElement.id
      )
    )

    con.executeAsync(
      STMT_DELETE_DATA_ELEMENT.bind(
        execution.ID,
        dataElement.id
      )
    )
  }

  override def allFilesOfExecution(id: UUID, onRead: (String, String) => Unit): Unit = {
    val sqlDeleteDependencies = ""
    val stmt = con.prepare(sqlDeleteDependencies)
      .bind(id)
    val rs = con.execute(stmt)
    val iter = rs.iterator()
    while (iter.hasNext) {
      val next = iter.next()
      onRead(next.getUUID("ID").toString, next.getString("Value"))
    }
  }


  override def allRelationshipBetweenDataElement(id: UUID, onRead: (String, String) => Unit): Unit = {
    val sqlDeleteDependencies = "SELECT  source, target from dfanalyzer.\"DependenciesOfDataElement\" WHERE \"executionID\" = ?"
    val stmt = con.prepare(sqlDeleteDependencies)
      .bind(id)
    val rs = con.execute(stmt)
    val iter = rs.iterator()
    while (iter.hasNext) {
      val next = iter.next()
      for (elem <- next.getSet("source", classOf[UUID]).toArray) {
        onRead(elem.toString, next.getUUID("target").toString)
      }
    }
  }
}
