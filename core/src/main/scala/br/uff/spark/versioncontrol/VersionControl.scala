package br.uff.spark.versioncontrol

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.util.concurrent.Executors

import br.uff.spark.Task
import br.uff.spark.advancedpipe.FileGroup
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

import scala.sys.process._

object VersionControl {

  private val instance = new VersionControl

  def getInstance: VersionControl = instance

  val isEnable = System.getenv("ENABLE_GIT") != null

}

class VersionControl private() {

  val log: Logger = org.apache.log4j.Logger.getLogger(classOf[VersionControl])
  var tempDir: File = null
  val executorService = Executors.newSingleThreadExecutor()
  var somethingWasWriting = false

  def checkExitStatus(exitCode: Int, onSuccess: String, onError: String): Unit = {
    if (exitCode == 0) log.info(onSuccess)
    else log.error(onError)
  }

  private def initLocalRepository(projectName: String): Unit = {
    val file = new File(new File("RootRepository"), projectName).getAbsoluteFile
    log.info("Creating the repository at: " + file)
    if (!file.exists) {
      file.mkdirs()
      var exitCode = Seq("git", "init", file.getAbsolutePath).!
      checkExitStatus(exitCode, "Success created repository", "Failed to create repository")
      exitCode = Process(Seq("git", "commit", "--allow-empty", "-m", "Zero Point"), file).!
      checkExitStatus(exitCode, "Success created the zero point commit", "Failed to create the zero point commit")
    }
  }

  private def createAExecutionBranch(projectName: String, executionID: UUID): File = {
    val file = new File(new File("RootRepository"), projectName).getAbsoluteFile
    val exitCode = Process(Seq("git", "branch", executionID.toString), file).!
    checkExitStatus(exitCode, "Success created the execution branch", "Failed to create the execution branch")
    file
  }

  private def cloneRepository(repository: File, executionID: UUID): Unit = {
    tempDir = new File("/tmp/git-" + executionID)
    val exitValue = Seq("git", "clone", "-b", executionID.toString, repository.getAbsolutePath, tempDir.getAbsolutePath, "--no-hardlinks").!
    checkExitStatus(exitValue, "Success clone the repository", "Failed to clone the repository")
  }

  def initAll(sparkContext: SparkContext): Unit = {
    initLocalRepository(sparkContext.appName)
    createAExecutionBranch(sparkContext.appName, sparkContext.executionID)
    cloneRepository(new File(new File("RootRepository"), sparkContext.appName).getAbsoluteFile, sparkContext.executionID)
  }

  def writeFileGroup(task: Task, fileGroup: FileGroup): Unit = {
    if (fileGroup.getName == null) {
      log.error("This file group doesn't have the git id")
      return
    }

    if (fileGroup.getFileElements.isEmpty) {
      log.error("The list of file can't be empty")
      return
    }

    executorService.submit(new Runnable {
      override def run(): Unit = {
        somethingWasWriting = true
        val target = new File(tempDir, task.description + "/" + fileGroup.getName)
        if (!target.exists()) {
          target.mkdirs()
        }
        for (fileElement <- fileGroup.getFileElements) {
          val file = Paths.get(target.getAbsolutePath, fileElement.getFilePath, fileElement.getFileName)
          if (Files.exists(file)) {
            Files.delete(file)
          }
          Files.createFile(file)
          FileUtils.copyInputStreamToFile(fileElement.getContents.toInputStream, file.toFile)
        }
        var exitValue = Process(Seq("git", "add", "-A"), tempDir).!
        checkExitStatus(exitValue, "Success add all files to commit", "Failed on add all files to commit")
        exitValue = Process(Seq("git", "commit", "-m", s"Task id: ${task.id} description: ${task.description}. Committing File Group: ${fileGroup.getName}"), tempDir).!
        checkExitStatus(exitValue, "Success commit the new files", "Failed on commit the new files")
      }
    })
  }

  def finish(): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        if (somethingWasWriting) {
          val exitValue = Process(Seq("git", "push"), tempDir).!
          checkExitStatus(exitValue, "Success push the new files", "Failed on push the new files")
          if (exitValue != 0) {
            return
          }
        }
        if (!scala.reflect.io.File(tempDir).deleteRecursively()) {
          log.error("Erro on Delete already used temp repository at: " + tempDir)
        }
      }
    })
    executorService.shutdown()
  }

}
