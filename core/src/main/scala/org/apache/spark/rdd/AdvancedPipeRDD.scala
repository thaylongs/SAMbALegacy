package org.apache.spark.rdd

import java.io._

import br.uff.spark.advancedpipe.{ExecutionPlanning, FileElement, FileGroup}
import br.uff.spark.vfs.MemoryFS
import br.uff.spark.{DataElement, TransformationType}
import com.google.common.io.Files
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * @author Thaylon Guedes Santos
  * @email thaylongs@gmail.com
  */
private[spark] class AdvancedPipeRDD(
                                      prev: RDD[FileGroup],
                                      f: (Map[String, Any], Seq[FileElement]) => ExecutionPlanning)
  extends RDD[FileGroup](prev) {

  setTransformationType(TransformationType.MAP)

  var executionPlan: ExecutionPlanning = null

  override def compute(split: Partition, context: TaskContext): Iterator[DataElement[FileGroup]] = {
    val input = firstParent[FileGroup].iterator(split, context)
    new Iterator[DataElement[FileGroup]] {

      override def hasNext: Boolean = input.hasNext

      override def next(): DataElement[FileGroup] = {
        //Next entry
        val nextEntry = input.next()
        val entry = nextEntry.value
        executionPlan = f.apply(entry.getExtraInfo.asScala.toMap, entry.getFileElements)

        //Making the ProcessBuilder
        val pb = new ProcessBuilder(executionPlan.command.toList.asJava)
        val currentEnvVars = pb.environment()
        executionPlan.env.foreach { case (variable, value) => currentEnvVars.put(variable, value) }
        pb.redirectErrorStream(executionPlan.redirectErrorStream)

        //Running the command
        val processedElement = runCommand(pb, entry)
        DataElement.of(processedElement, task, task.isIgnored, nextEntry)
      }
    }
  }

  def runCommand(pb: ProcessBuilder, fileGroup: FileGroup): FileGroup = {
    val taskDirectory = Files.createTempDir()
    pb.directory(taskDirectory)
    logDebug("Task Directory = " + taskDirectory)
    val memoryFS = new MemoryFS(fileGroup)

    try {
      memoryFS.mount(taskDirectory.toPath, false, log.isDebugEnabled)

      val proc = pb.start()

      /* Reading output of Command */
      if (!executionPlan.redirectErrorStream)
        newThreadReadOutput(
          s"stderr reader for ${executionPlan.command.mkString(" ")} in $taskDirectory",
          proc.getErrorStream,
          (line) => executionPlan.onReadErrorLine(line)
        )

      newThreadReadOutput(
        s"stdin reader for ${executionPlan.command.mkString(" ")} in $taskDirectory",
        proc.getInputStream,
        (line) => executionPlan.onReadLine(line)
      )

      /*End - Reading output of Command */
      val exitStatus = proc.waitFor()
      if (exitStatus != 0) throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
        s"Command ran: " + executionPlan.command.mkString(" ") + s" in $taskDirectory")

      //Processing Results
      val result = memoryFS.toFileGroup(executionPlan.filterFilesForGeneratedRDD.apply(_))
      if (executionPlan.getExtraInfoForGeneratedRDD != null)
        result.setExtraInfo(executionPlan.getExtraInfoForGeneratedRDD.apply().asJava)
      else
        result.setExtraInfo(fileGroup.getExtraInfo)
      result
    } catch {
      case t: Throwable => {
        throw t
      }
    } finally {
      memoryFS.umount()
      taskDirectory.delete()
    }

  }

  //TODO Throw the possible exception
  def newThreadReadOutput(threadName: String, inputStream: InputStream, onRead: (String) => Unit): Thread = {
    val result = new Thread(threadName) {
      override def run(): Unit = {
        try
            for (line <- Source.fromInputStream(inputStream)(executionPlan.encoding).getLines) {
              onRead(line)
            }
        catch {
          case t: Throwable => t.printStackTrace()
        } finally inputStream.close()
      }
    }
    result.start()
    result
  }

  override protected def getPartitions: Array[Partition] = firstParent[FileGroup].partitions
}
