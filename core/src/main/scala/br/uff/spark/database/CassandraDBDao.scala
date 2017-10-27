package br.uff.spark.database

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID

import br.uff.spark.{DataElement, DataSource, Execution, Task}

import scala.collection.JavaConverters._

class CassandraDBDao(val execution: Execution) extends DataBaseBasicMethods {

  val con = DataSource.getConnection

  override def insertTask(task: Task): Unit = {
    var sql = "INSERT INTO dfanalyzer.\"Task\" (\"executionID\", id, description, \"transformationType\") VALUES(?, ?, ?, ?);"
    var stmp = con.prepare(sql)
      .bind(
        execution.ID,
        task.id,
        task.description,
        task.transformationType.toString
      )
    con.execute(stmp)

    sql = "INSERT INTO dfanalyzer.\"DependenciesOfTask\" (\"executionID\", target, \"source\") VALUES(?, ?, ?);"
    stmp = con.prepare(sql)
      .bind(
        execution.ID,
        task.id,
        task.dependenciesIDS.toSet.asJava
      )
    con.execute(stmp)
  }


  override def close(): Unit = {
    DataSource.close()
  }

  override def updateExecution(execution: Execution): Unit = {
    val sql = "UPDATE dfanalyzer.\"Execution\" SET  \"EndTime\"=?  WHERE ID=?;"

    val stmp = con.prepare(sql)
      .bind(
        Timestamp.from(execution.endTime.toInstant(ZonedDateTime.now(ZoneId.systemDefault()).getOffset)),
        execution.ID
      )
    con.execute(stmp)

  }

  override def init(): Unit = {

    val sqlInsertExecution = "INSERT INTO dfanalyzer.\"Execution\"(id, \"StartTime\", \"EndTime\", \"AplicationName\") VALUES (?, ?, ?, ?);"
    val stmp = con.prepare(sqlInsertExecution)
      .bind(
        execution.ID,
        Timestamp.from(execution.startTime.toInstant(ZonedDateTime.now(ZoneId.systemDefault()).getOffset)),
        null,
        execution.name
      )
    con.execute(stmp)

  }

  override def insertDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    val sqlInsertDataElement = "INSERT INTO dfanalyzer.\"DataElement\" (id, value, \"executionID\") VALUES(? , ? , ?);"
    val stmp = con.prepare(sqlInsertDataElement)
      .bind(
        dataElement.id,
        dataElement.value.toString,
        execution.ID
      )
    con.execute(stmp)

    if (!dataElement.dependenciesIDS.isEmpty)
      insertDependencies(dataElement)
  }

  override def insertDependencies(dataElement: DataElement[_ <: Any]): Unit = {
    val sqlInsertDataElementDependencies = "INSERT INTO dfanalyzer.\"DependenciesOfDataElement\" (source, target, task, \"executionID\") VALUES(?, ?, ?,?);"
    val stmp = con.prepare(sqlInsertDataElementDependencies)
      .bind(
        dataElement.dependenciesIDS.toSet.asJava,
        dataElement.id,
        dataElement.task.id,
        execution.ID
      )
    con.execute(stmp)
  }

  override def insertDependencieOfDataElement(dataElement: DataElement[_ <: Any], id: UUID): Unit = {
    val sqlInsertDataElementDependencies = "UPDATE dfanalyzer.\"DependenciesOfDataElement\" SET source = source + ?  where target=?  and task=?  and \"executionID\" =?"
    val stmp = con.prepare(sqlInsertDataElementDependencies)
      .bind(
        Set(id).asJava,
        dataElement.id,
        dataElement.task.id,
        execution.ID
      )
    con.execute(stmp)
  }

  override def updateDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    val sql = "UPDATE dfanalyzer.\"DataElement\" SET value = ? WHERE \"executionID\"=? and id=?;"
    val stmp = con.prepare(sql)
      .bind(
        dataElement.value.toString,
        execution.ID,
        dataElement.id
      )
    con.execute(stmp)
  }


  override def deleteDataElement(dataElement: DataElement[_ <: Any]): Unit = {
    val sqlDeleteDependencies = "DELETE FROM  dfanalyzer.\"DependenciesOfDataElement\" WHERE \"executionID\"=? and task=? and target=?"
    var stmp = con.prepare(sqlDeleteDependencies)
      .bind(
        execution.ID,
        dataElement.task.id,
        dataElement.id
      )
    con.execute(stmp)

    val sqlDeleteDataElement = "DELETE FROM dfanalyzer.\"DataElement\" WHERE \"executionID\"=?  and  id=?"
    stmp = con.prepare(sqlDeleteDataElement)
      .bind(
        execution.ID,
        dataElement.id
      )
    con.execute(stmp)
  }


  override def allFilesOfExecution(id: UUID, onRead: (String, String) => Unit) = {
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


  override def allRelationshipBetweenDataElement(id: UUID, onRead: (String, String) => Unit) = {
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
