/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.status

import java.io.File
import java.util.{Date, Properties}

import scala.collection.JavaConverters._
import scala.reflect.{classTag, ClassTag}

import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.status.api.v1
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore._

class AppStatusListenerSuite extends SparkFunSuite with BeforeAndAfter {

  private var time: Long = _
  private var testDir: File = _
  private var store: KVStore = _

  before {
    time = 0L
    testDir = Utils.createTempDir()
    store = KVUtils.open(testDir, getClass().getName())
  }

  after {
    store.close()
    Utils.deleteRecursively(testDir)
  }

  test("scheduler events") {
    val listener = new AppStatusListener(store)

    // Start the application.
    time += 1
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      None))

    check[ApplicationInfoWrapper]("id") { app =>
      assert(app.info.name === "name")
      assert(app.info.id === "id")
      assert(app.info.attempts.size === 1)

      val attempt = app.info.attempts.head
      assert(attempt.attemptId === Some("attempt"))
      assert(attempt.startTime === new Date(time))
      assert(attempt.lastUpdated === new Date(time))
      assert(attempt.endTime.getTime() === -1L)
      assert(attempt.sparkUser === "user")
      assert(!attempt.completed)
    }

    // Start a couple of executors.
    time += 1
    val execIds = Array("1", "2")

    execIds.foreach { id =>
      listener.onExecutorAdded(SparkListenerExecutorAdded(time, id,
        new ExecutorInfo(s"$id.example.com", 1, Map())))
    }

    execIds.foreach { id =>
      check[ExecutorSummaryWrapper](id) { exec =>
        assert(exec.info.id === id)
        assert(exec.info.hostPort === s"$id.example.com")
        assert(exec.info.isActive)
      }
    }

    // Start a job with 2 stages / 4 tasks each
    time += 1
    val stages = Seq(
      new StageInfo(1, 0, "stage1", 4, Nil, Nil, "details1"),
      new StageInfo(2, 0, "stage2", 4, Nil, Seq(1), "details2"))

    val jobProps = new Properties()
    jobProps.setProperty(SparkContext.SPARK_JOB_GROUP_ID, "jobGroup")
    jobProps.setProperty("spark.scheduler.pool", "schedPool")

    listener.onJobStart(SparkListenerJobStart(1, time, stages, jobProps))

    check[JobDataWrapper](1) { job =>
      assert(job.info.jobId === 1)
      assert(job.info.name === stages.last.name)
      assert(job.info.description === None)
      assert(job.info.status === JobExecutionStatus.RUNNING)
      assert(job.info.submissionTime === Some(new Date(time)))
      assert(job.info.jobGroup === Some("jobGroup"))
    }

    stages.foreach { info =>
      check[StageDataWrapper](key(info)) { stage =>
        assert(stage.info.status === v1.StageStatus.PENDING)
        assert(stage.jobIds === Set(1))
      }
    }

    // Submit stage 1
    time += 1
    stages.head.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.head, jobProps))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 1)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.status === v1.StageStatus.ACTIVE)
      assert(stage.info.submissionTime === Some(new Date(stages.head.submissionTime.get)))
      assert(stage.info.schedulingPool === "schedPool")
    }

    // Start tasks from stage 1
    time += 1
    var _taskIdTracker = -1L
    def nextTaskId(): Long = {
      _taskIdTracker += 1
      _taskIdTracker
    }

    def createTasks(count: Int, time: Long): Seq[TaskInfo] = {
      (1 to count).map { id =>
        val exec = execIds(id.toInt % execIds.length)
        val taskId = nextTaskId()
        new TaskInfo(taskId, taskId.toInt, 1, time, exec, s"$exec.example.com",
          TaskLocality.PROCESS_LOCAL, id % 2 == 0)
      }
    }

    val s1Tasks = createTasks(4, time)
    s1Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptId, task))
    }

    assert(store.count(classOf[TaskDataWrapper]) === s1Tasks.size)

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveTasks === s1Tasks.size)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numActiveTasks === s1Tasks.size)
      assert(stage.info.firstTaskLaunchedTime === Some(new Date(s1Tasks.head.launchTime)))
    }

    s1Tasks.foreach { task =>
      check[TaskDataWrapper](task.taskId) { wrapper =>
        assert(wrapper.info.taskId === task.taskId)
        assert(wrapper.info.index === task.index)
        assert(wrapper.info.attempt === task.attemptNumber)
        assert(wrapper.info.launchTime === new Date(task.launchTime))
        assert(wrapper.info.executorId === task.executorId)
        assert(wrapper.info.host === task.host)
        assert(wrapper.info.status === task.status)
        assert(wrapper.info.taskLocality === task.taskLocality.toString())
        assert(wrapper.info.speculative === task.speculative)
      }
    }

    // Send executor metrics update. Only update one metric to avoid a lot of boilerplate code.
    s1Tasks.foreach { task =>
      val accum = new AccumulableInfo(1L, Some(InternalAccumulator.MEMORY_BYTES_SPILLED),
        Some(1L), None, true, false, None)
      listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate(
        task.executorId,
        Seq((task.taskId, stages.head.stageId, stages.head.attemptId, Seq(accum)))))
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.memoryBytesSpilled === s1Tasks.size)
    }

    val execs = store.view(classOf[ExecutorStageSummaryWrapper]).index("stage")
      .first(key(stages.head)).last(key(stages.head)).asScala.toSeq
    assert(execs.size > 0)
    execs.foreach { exec =>
      assert(exec.info.memoryBytesSpilled === s1Tasks.size / 2)
    }

    // Fail one of the tasks, re-start it.
    time += 1
    s1Tasks.head.markFinished(TaskState.FAILED, time)
    listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptId,
      "taskType", TaskResultLost, s1Tasks.head, null))

    time += 1
    val reattempt = {
      val orig = s1Tasks.head
      // Task reattempts have a different ID, but the same index as the original.
      new TaskInfo(nextTaskId(), orig.index, orig.attemptNumber + 1, time, orig.executorId,
        s"${orig.executorId}.example.com", TaskLocality.PROCESS_LOCAL, orig.speculative)
    }
    listener.onTaskStart(SparkListenerTaskStart(stages.head.stageId, stages.head.attemptId,
      reattempt))

    assert(store.count(classOf[TaskDataWrapper]) === s1Tasks.size + 1)

    check[JobDataWrapper](1) { job =>
      assert(job.info.numFailedTasks === 1)
      assert(job.info.numActiveTasks === s1Tasks.size)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numFailedTasks === 1)
      assert(stage.info.numActiveTasks === s1Tasks.size)
    }

    check[TaskDataWrapper](s1Tasks.head.taskId) { task =>
      assert(task.info.status === s1Tasks.head.status)
      assert(task.info.duration === Some(s1Tasks.head.duration))
      assert(task.info.errorMessage == Some(TaskResultLost.toErrorString))
    }

    check[TaskDataWrapper](reattempt.taskId) { task =>
      assert(task.info.index === s1Tasks.head.index)
      assert(task.info.attempt === reattempt.attemptNumber)
    }

    // Succeed all tasks in stage 1.
    val pending = s1Tasks.drop(1) ++ Seq(reattempt)

    val s1Metrics = TaskMetrics.empty
    s1Metrics.setExecutorCpuTime(2L)
    s1Metrics.setExecutorRunTime(4L)

    time += 1
    pending.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.head.stageId, stages.head.attemptId,
        "taskType", Success, task, s1Metrics))
    }

    check[JobDataWrapper](1) { job =>
      assert(job.info.numFailedTasks === 1)
      assert(job.info.numActiveTasks === 0)
      assert(job.info.numCompletedTasks === pending.size)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.numFailedTasks === 1)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === pending.size)
    }

    pending.foreach { task =>
      check[TaskDataWrapper](task.taskId) { wrapper =>
        assert(wrapper.info.errorMessage === None)
        assert(wrapper.info.taskMetrics.get.executorCpuTime === 2L)
        assert(wrapper.info.taskMetrics.get.executorRunTime === 4L)
      }
    }

    assert(store.count(classOf[TaskDataWrapper]) === pending.size + 1)

    // End stage 1.
    time += 1
    stages.head.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(stages.head))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 0)
      assert(job.info.numCompletedStages === 1)
    }

    check[StageDataWrapper](key(stages.head)) { stage =>
      assert(stage.info.status === v1.StageStatus.COMPLETE)
      assert(stage.info.numFailedTasks === 1)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === pending.size)
    }

    // Submit stage 2.
    time += 1
    stages.last.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(stages.last, jobProps))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 1)
    }

    check[StageDataWrapper](key(stages.last)) { stage =>
      assert(stage.info.status === v1.StageStatus.ACTIVE)
      assert(stage.info.submissionTime === Some(new Date(stages.last.submissionTime.get)))
    }

    // Start and fail all tasks of stage 2.
    time += 1
    val s2Tasks = createTasks(4, time)
    s2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(stages.last.stageId, stages.last.attemptId, task))
    }

    time += 1
    s2Tasks.foreach { task =>
      task.markFinished(TaskState.FAILED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(stages.last.stageId, stages.last.attemptId,
        "taskType", TaskResultLost, task, null))
    }

    check[JobDataWrapper](1) { job =>
      assert(job.info.numFailedTasks === 1 + s2Tasks.size)
      assert(job.info.numActiveTasks === 0)
    }

    check[StageDataWrapper](key(stages.last)) { stage =>
      assert(stage.info.numFailedTasks === s2Tasks.size)
      assert(stage.info.numActiveTasks === 0)
    }

    // Fail stage 2.
    time += 1
    stages.last.completionTime = Some(time)
    stages.last.failureReason = Some("uh oh")
    listener.onStageCompleted(SparkListenerStageCompleted(stages.last))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numCompletedStages === 1)
      assert(job.info.numFailedStages === 1)
    }

    check[StageDataWrapper](key(stages.last)) { stage =>
      assert(stage.info.status === v1.StageStatus.FAILED)
      assert(stage.info.numFailedTasks === s2Tasks.size)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === 0)
    }

    // - Re-submit stage 2, all tasks, and succeed them and the stage.
    val oldS2 = stages.last
    val newS2 = new StageInfo(oldS2.stageId, oldS2.attemptId + 1, oldS2.name, oldS2.numTasks,
      oldS2.rddInfos, oldS2.parentIds, oldS2.details, oldS2.taskMetrics)

    time += 1
    newS2.submissionTime = Some(time)
    listener.onStageSubmitted(SparkListenerStageSubmitted(newS2, jobProps))
    assert(store.count(classOf[StageDataWrapper]) === 3)

    val newS2Tasks = createTasks(4, time)

    newS2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(newS2.stageId, newS2.attemptId, task))
    }

    time += 1
    newS2Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(newS2.stageId, newS2.attemptId, "taskType", Success,
        task, null))
    }

    time += 1
    newS2.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(newS2))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numActiveStages === 0)
      assert(job.info.numFailedStages === 1)
      assert(job.info.numCompletedStages === 2)
    }

    check[StageDataWrapper](key(newS2)) { stage =>
      assert(stage.info.status === v1.StageStatus.COMPLETE)
      assert(stage.info.numActiveTasks === 0)
      assert(stage.info.numCompleteTasks === newS2Tasks.size)
    }

    // End job.
    time += 1
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    check[JobDataWrapper](1) { job =>
      assert(job.info.status === JobExecutionStatus.SUCCEEDED)
    }

    // Submit a second job that re-uses stage 1 and stage 2. Stage 1 won't be re-run, but
    // stage 2 will. In any case, the DAGScheduler creates new info structures that are copies
    // of the old stages, so mimic that behavior here. The "new" stage 1 is submitted without
    // a submission time, which means it is "skipped", and the stage 2 re-execution should not
    // change the stats of the already finished job.
    time += 1
    val j2Stages = Seq(
      new StageInfo(3, 0, "stage1", 4, Nil, Nil, "details1"),
      new StageInfo(4, 0, "stage2", 4, Nil, Seq(3), "details2"))
    j2Stages.last.submissionTime = Some(time)
    listener.onJobStart(SparkListenerJobStart(2, time, j2Stages, null))
    assert(store.count(classOf[JobDataWrapper]) === 2)

    listener.onStageSubmitted(SparkListenerStageSubmitted(j2Stages.head, jobProps))
    listener.onStageCompleted(SparkListenerStageCompleted(j2Stages.head))
    listener.onStageSubmitted(SparkListenerStageSubmitted(j2Stages.last, jobProps))
    assert(store.count(classOf[StageDataWrapper]) === 5)

    time += 1
    val j2s2Tasks = createTasks(4, time)

    j2s2Tasks.foreach { task =>
      listener.onTaskStart(SparkListenerTaskStart(j2Stages.last.stageId, j2Stages.last.attemptId,
        task))
    }

    time += 1
    j2s2Tasks.foreach { task =>
      task.markFinished(TaskState.FINISHED, time)
      listener.onTaskEnd(SparkListenerTaskEnd(j2Stages.last.stageId, j2Stages.last.attemptId,
        "taskType", Success, task, null))
    }

    time += 1
    j2Stages.last.completionTime = Some(time)
    listener.onStageCompleted(SparkListenerStageCompleted(j2Stages.last))

    time += 1
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))

    check[JobDataWrapper](1) { job =>
      assert(job.info.numCompletedStages === 2)
      assert(job.info.numCompletedTasks === s1Tasks.size + s2Tasks.size)
    }

    check[JobDataWrapper](2) { job =>
      assert(job.info.status === JobExecutionStatus.SUCCEEDED)
      assert(job.info.numCompletedStages === 1)
      assert(job.info.numCompletedTasks === j2s2Tasks.size)
      assert(job.info.numSkippedStages === 1)
      assert(job.info.numSkippedTasks === s1Tasks.size)
    }

    // Blacklist an executor.
    time += 1
    listener.onExecutorBlacklisted(SparkListenerExecutorBlacklisted(time, "1", 42))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(exec.info.isBlacklisted)
    }

    time += 1
    listener.onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted(time, "1"))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(!exec.info.isBlacklisted)
    }

    // Blacklist a node.
    time += 1
    listener.onNodeBlacklisted(SparkListenerNodeBlacklisted(time, "1.example.com", 2))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(exec.info.isBlacklisted)
    }

    time += 1
    listener.onNodeUnblacklisted(SparkListenerNodeUnblacklisted(time, "1.example.com"))
    check[ExecutorSummaryWrapper]("1") { exec =>
      assert(!exec.info.isBlacklisted)
    }

    // Stop executors.
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(41L, "1", "Test"))
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(41L, "2", "Test"))

    Seq("1", "2").foreach { id =>
      check[ExecutorSummaryWrapper](id) { exec =>
        assert(exec.info.id === id)
        assert(!exec.info.isActive)
      }
    }

    // End the application.
    listener.onApplicationEnd(SparkListenerApplicationEnd(42L))

    check[ApplicationInfoWrapper]("id") { app =>
      assert(app.info.name === "name")
      assert(app.info.id === "id")
      assert(app.info.attempts.size === 1)

      val attempt = app.info.attempts.head
      assert(attempt.attemptId === Some("attempt"))
      assert(attempt.startTime === new Date(1L))
      assert(attempt.lastUpdated === new Date(42L))
      assert(attempt.endTime === new Date(42L))
      assert(attempt.duration === 41L)
      assert(attempt.sparkUser === "user")
      assert(attempt.completed)
    }
  }

  test("storage events") {
    val listener = new AppStatusListener(store)
    val maxMemory = 42L

    // Register a couple of block managers.
    val bm1 = BlockManagerId("1", "1.example.com", 42)
    val bm2 = BlockManagerId("2", "2.example.com", 84)
    Seq(bm1, bm2).foreach { bm =>
      listener.onExecutorAdded(SparkListenerExecutorAdded(1L, bm.executorId,
        new ExecutorInfo(bm.host, 1, Map())))
      listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(1L, bm, maxMemory))
      check[ExecutorSummaryWrapper](bm.executorId) { exec =>
        assert(exec.info.maxMemory === maxMemory)
      }
    }

    val rdd1b1 = RDDBlockId(1, 1)
    val level = StorageLevel.MEMORY_AND_DISK

    // Submit a stage and make sure the RDD is recorded.
    val rddInfo = new RDDInfo(rdd1b1.rddId, "rdd1", 2, level, Nil)
    val stage = new StageInfo(1, 0, "stage1", 4, Seq(rddInfo), Nil, "details1")
    listener.onStageSubmitted(SparkListenerStageSubmitted(stage, new Properties()))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.name === rddInfo.name)
      assert(wrapper.info.numPartitions === rddInfo.numPartitions)
      assert(wrapper.info.storageLevel === rddInfo.storageLevel.description)
    }

    // Add partition 1 replicated on two block managers.
    listener.onBlockUpdated(SparkListenerBlockUpdated(BlockUpdatedInfo(bm1, rdd1b1, level, 1L, 1L)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.memoryUsed === 1L)
      assert(wrapper.info.diskUsed === 1L)

      assert(wrapper.info.dataDistribution.isDefined)
      assert(wrapper.info.dataDistribution.get.size === 1)

      val dist = wrapper.info.dataDistribution.get.head
      assert(dist.address === bm1.hostPort)
      assert(dist.memoryUsed === 1L)
      assert(dist.diskUsed === 1L)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      assert(wrapper.info.partitions.isDefined)
      assert(wrapper.info.partitions.get.size === 1)

      val part = wrapper.info.partitions.get.head
      assert(part.blockName === rdd1b1.name)
      assert(part.storageLevel === level.description)
      assert(part.memoryUsed === 1L)
      assert(part.diskUsed === 1L)
      assert(part.executors === Seq(bm1.executorId))
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === 1L)
      assert(exec.info.diskUsed === 1L)
    }

    listener.onBlockUpdated(SparkListenerBlockUpdated(BlockUpdatedInfo(bm2, rdd1b1, level, 1L, 1L)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.memoryUsed === 2L)
      assert(wrapper.info.diskUsed === 2L)
      assert(wrapper.info.dataDistribution.get.size === 2L)
      assert(wrapper.info.partitions.get.size === 1L)

      val dist = wrapper.info.dataDistribution.get.find(_.address == bm2.hostPort).get
      assert(dist.memoryUsed === 1L)
      assert(dist.diskUsed === 1L)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      val part = wrapper.info.partitions.get(0)
      assert(part.memoryUsed === 2L)
      assert(part.diskUsed === 2L)
      assert(part.executors === Seq(bm1.executorId, bm2.executorId))
    }

    check[ExecutorSummaryWrapper](bm2.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === 1L)
      assert(exec.info.diskUsed === 1L)
    }

    // Add a second partition only to bm 1.
    val rdd1b2 = RDDBlockId(1, 2)
    listener.onBlockUpdated(SparkListenerBlockUpdated(BlockUpdatedInfo(bm1, rdd1b2, level,
      3L, 3L)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.memoryUsed === 5L)
      assert(wrapper.info.diskUsed === 5L)
      assert(wrapper.info.dataDistribution.get.size === 2L)
      assert(wrapper.info.partitions.get.size === 2L)

      val dist = wrapper.info.dataDistribution.get.find(_.address == bm1.hostPort).get
      assert(dist.memoryUsed === 4L)
      assert(dist.diskUsed === 4L)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      val part = wrapper.info.partitions.get.find(_.blockName === rdd1b2.name).get
      assert(part.storageLevel === level.description)
      assert(part.memoryUsed === 3L)
      assert(part.diskUsed === 3L)
      assert(part.executors === Seq(bm1.executorId))
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 2L)
      assert(exec.info.memoryUsed === 4L)
      assert(exec.info.diskUsed === 4L)
    }

    // Remove block 1 from bm 1.
    listener.onBlockUpdated(SparkListenerBlockUpdated(BlockUpdatedInfo(bm1, rdd1b1,
      StorageLevel.NONE, 1L, 1L)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.memoryUsed === 4L)
      assert(wrapper.info.diskUsed === 4L)
      assert(wrapper.info.dataDistribution.get.size === 2L)
      assert(wrapper.info.partitions.get.size === 2L)

      val dist = wrapper.info.dataDistribution.get.find(_.address == bm1.hostPort).get
      assert(dist.memoryUsed === 3L)
      assert(dist.diskUsed === 3L)
      assert(dist.memoryRemaining === maxMemory - dist.memoryUsed)

      val part = wrapper.info.partitions.get.find(_.blockName === rdd1b1.name).get
      assert(part.storageLevel === level.description)
      assert(part.memoryUsed === 1L)
      assert(part.diskUsed === 1L)
      assert(part.executors === Seq(bm2.executorId))
    }

    check[ExecutorSummaryWrapper](bm1.executorId) { exec =>
      assert(exec.info.rddBlocks === 1L)
      assert(exec.info.memoryUsed === 3L)
      assert(exec.info.diskUsed === 3L)
    }

    // Remove block 2 from bm 2. This should leave only block 2 info in the store.
    listener.onBlockUpdated(SparkListenerBlockUpdated(BlockUpdatedInfo(bm2, rdd1b1,
      StorageLevel.NONE, 1L, 1L)))

    check[RDDStorageInfoWrapper](rdd1b1.rddId) { wrapper =>
      assert(wrapper.info.memoryUsed === 3L)
      assert(wrapper.info.diskUsed === 3L)
      assert(wrapper.info.dataDistribution.get.size === 1L)
      assert(wrapper.info.partitions.get.size === 1L)
      assert(wrapper.info.partitions.get(0).blockName === rdd1b2.name)
    }

    check[ExecutorSummaryWrapper](bm2.executorId) { exec =>
      assert(exec.info.rddBlocks === 0L)
      assert(exec.info.memoryUsed === 0L)
      assert(exec.info.diskUsed === 0L)
    }

    // Unpersist RDD1.
    listener.onUnpersistRDD(SparkListenerUnpersistRDD(rdd1b1.rddId))
        intercept[NoSuchElementException] {
      check[RDDStorageInfoWrapper](rdd1b1.rddId) { _ => () }
    }

  }

  private def key(stage: StageInfo): Array[Int] = Array(stage.stageId, stage.attemptId)

  private def check[T: ClassTag](key: Any)(fn: T => Unit): Unit = {
    val value = store.read(classTag[T].runtimeClass, key).asInstanceOf[T]
    fn(value)
  }

}
