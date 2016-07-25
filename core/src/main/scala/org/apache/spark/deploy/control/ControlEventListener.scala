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

package org.apache.spark.deploy.control

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkStageWeightSubmitted, _}
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData._

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

/**
  *
  * Created by Matteo on 20/07/2016.
  *
  *
  * All access to the data structures in this class must be synchronized on the
  * class, since the UI thread and the EventBus loop may otherwise be reading and
  * updating the internal data structures concurrently.
  */
class ControlEventListener(conf: SparkConf) extends SparkListener with Logging {

  // Application:
  @volatile var startTime = -1L
  @volatile var endTime = -1L

  val DEADLINE: Int = 1000
  val ALPHA: Double = 0.8
  val NOMINAL_RATE: Double = 10000.0

  // Jobs:
  val activeJobs = new HashMap[Int, JobUIData]
  val jobIdToData = new HashMap[Int, JobUIData]

  val deadlineJobs = new HashMap[Int, Long]
  val jobIdToController = new HashMap[Int, ControllerJob]

  // Stages:
  val pendingStages = new HashMap[Int, StageInfo]
  val activeStages = new HashMap[Int, StageInfo]
  val completedStages = ListBuffer[StageInfo]()
  val skippedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()
  val stageIdToData = new HashMap[(Int, Int), StageUIData]
  val stageIdToInfo = new HashMap[Int, StageInfo]
  val stageIdToActiveJobIds = new HashMap[Int, HashSet[Int]]

  val stageIdToDeadline = new HashMap[Int, Long]
  val stageIdToCore = new HashMap[Int, Int]

  var firstStageId: Int = -1

  // Executor
  var executorAvailable = Set[String]()
  var execIdToStageId = new HashMap[String, Long].withDefaultValue(0)
  var stageIdToExecId = new HashMap[Long, Set[String]].withDefaultValue(Set())
  var executorIdToInfo = new HashMap[String, ExecutorInfo]

  // Controller
  var controllerJob = new HashMap[Int, ControllerJob]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobGroup = for (
      props <- Option(jobStart.properties);
      group <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    ) yield group
    val jobData: JobUIData =
      new JobUIData(
        jobId = jobStart.jobId,
        submissionTime = Option(jobStart.time).filter(_ >= 0),
        stageIds = jobStart.stageIds,
        jobGroup = jobGroup,
        status = JobExecutionStatus.RUNNING)

    jobStart.stageInfos.foreach(x => pendingStages(x.stageId) = x)
    // Compute (a potential underestimate of) the number of tasks that will be run by this job.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    jobData.numTasks = {
      val allStages = jobStart.stageInfos
      val missingStages = allStages.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }
    jobIdToData(jobStart.jobId) = jobData
    deadlineJobs(jobStart.jobId) = DEADLINE
    activeJobs(jobStart.jobId) = jobData
    for (stageId <- jobStart.stageIds) {
      stageIdToActiveJobIds.getOrElseUpdate(stageId, new HashSet[Int]).add(jobStart.jobId)
    }
    // If there's no information for a stage, store the StageInfo received from the scheduler
    // so that we can display stage descriptions for pending stages:
    for (stageInfo <- jobStart.stageInfos) {
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, stageInfo)
      stageIdToData.getOrElseUpdate((stageInfo.stageId, stageInfo.attemptId), new StageUIData)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobData = activeJobs.remove(jobEnd.jobId).getOrElse {
      logWarning(s"Job completed for unknown job ${jobEnd.jobId}")
      new JobUIData(jobId = jobEnd.jobId)
    }
    jobData.completionTime = Option(jobEnd.time).filter(_ >= 0)

    jobData.stageIds.foreach(pendingStages.remove)
    for (stageId <- jobData.stageIds) {
      stageIdToActiveJobIds.get(stageId).foreach { jobsUsingStage =>
        jobsUsingStage.remove(jobEnd.jobId)
        if (jobsUsingStage.isEmpty) {
          stageIdToActiveJobIds.remove(stageId)
        }
        stageIdToInfo.get(stageId).foreach { stageInfo =>
          if (stageInfo.submissionTime.isEmpty) {
            // if this stage is pending, it won't complete, so mark it as "skipped":
            skippedStages += stageInfo
            jobData.numSkippedStages += 1
            jobData.numSkippedTasks += stageInfo.numTasks
          }
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val stage = stageCompleted.stageInfo
    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), {
      logWarning("Stage completed for unknown stage " + stage.stageId)
      new StageUIData
    })

    for ((id, info) <- stageCompleted.stageInfo.accumulables) {
      stageData.accumulables(id) = info
    }


    activeStages.remove(stage.stageId)
    if (stage.failureReason.isEmpty) {
      completedStages += stage
    } else {
      failedStages += stage
    }

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages -= 1
      if (stage.failureReason.isEmpty) {
        if (!stage.submissionTime.isEmpty) {
          jobData.completedStageIndices.add(stage.stageId)
        }
      } else {
        jobData.numFailedStages += 1
      }
    }
  }

  override def onStageWeightSubmitted
  (stageSubmitted: SparkStageWeightSubmitted): Unit = synchronized {
    val stage = stageSubmitted.stageInfo
    val stageWeight = stageSubmitted.weight
    val jobId = stageIdToActiveJobIds(stage.stageId)

    var recordInput: Long = 0
    stage.parentIds.foreach(x => recordInput += stageIdToData(x, 0).outputRecords)
    if (firstStageId == -1) {
      firstStageId = stage.stageId
      val controller = new ControllerJob(
        stage.numTasks, deadlineJobs(jobId.head), ALPHA, NOMINAL_RATE)
      stageIdToDeadline(stage.stageId) = controller.computeDeadlineFirstStage(stage, stageWeight)
      stageIdToCore(stage.stageId) = controller.computeCoreFirstStage(stage)
      controller.askMasterNeededCore("", firstStageId, stageIdToCore(firstStageId), "")
      jobIdToController(jobId.head) = controller
    }

    val controller = jobIdToController(jobId.head)
    val deadlineStage = controller.computeDeadlineStage(stage, stageWeight)
    stageIdToDeadline(stage.stageId) = deadlineStage
    stageIdToCore(stage.stageId) = controller.computeCoreStage(deadlineStage, stageWeight)
    logInfo("Submitted stage: %s with deadline: %s and core: %s".format(
      stage.stageId, stageIdToDeadline(stage.stageId), stageIdToCore(stage.stageId)))

    activeStages(stage.stageId) = stage
    pendingStages.remove(stage.stageId)
    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.pool", SparkUI.DEFAULT_POOL_NAME)
    }.getOrElse(SparkUI.DEFAULT_POOL_NAME)

    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), new StageUIData)
    stageData.schedulingPool = poolName

    stageData.description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages += 1

      // If a stage retries again, it should be removed from completedStageIndices set
      jobData.completedStageIndices.remove(stage.stageId)
    }

  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val taskInfo = taskStart.taskInfo
    if (taskInfo != null) {
      val stageData = stageIdToData.getOrElseUpdate((taskStart.stageId, taskStart.stageAttemptId), {
        logWarning("Task start for unknown stage " + taskStart.stageId)
        new StageUIData
      })
      stageData.numActiveTasks += 1
      stageData.taskData.put(taskInfo.taskId, new TaskUIData(taskInfo))
    }
    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskStart.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveTasks += 1
    }
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    if (info != null && taskEnd.stageAttemptId != -1) {
      val stageData = stageIdToData.getOrElseUpdate((taskEnd.stageId, taskEnd.stageAttemptId), {
        logWarning("Task end for unknown stage " + taskEnd.stageId)
        new StageUIData
      })

      for (accumulableInfo <- info.accumulables) {
        stageData.accumulables(accumulableInfo.id) = accumulableInfo
      }

      val execSummaryMap = stageData.executorSummary
      val execSummary = execSummaryMap.getOrElseUpdate(info.executorId, new ExecutorSummary)

      taskEnd.reason match {
        case Success =>
          execSummary.succeededTasks += 1
        case _ =>
          execSummary.failedTasks += 1
      }
      execSummary.taskTime += info.duration
      stageData.numActiveTasks -= 1

      val (errorMessage, metrics): (Option[String], Option[TaskMetrics]) =
        taskEnd.reason match {
          case org.apache.spark.Success =>
            stageData.completedIndices.add(info.index)
            stageData.numCompleteTasks += 1
            (None, Option(taskEnd.taskMetrics))
          case e: ExceptionFailure => // Handle ExceptionFailure because we might have metrics
            stageData.numFailedTasks += 1
            (Some(e.toErrorString), e.metrics)
          case e: TaskFailedReason => // All other failure cases
            stageData.numFailedTasks += 1
            (Some(e.toErrorString), None)
        }

      if (!metrics.isEmpty) {
        val oldMetrics = stageData.taskData.get(info.taskId).flatMap(_.taskMetrics)
        updateAggregateMetrics(stageData, info.executorId, metrics.get, oldMetrics)
      }

      val taskData = stageData.taskData.getOrElseUpdate(info.taskId, new TaskUIData(info))
      taskData.taskInfo = info
      taskData.taskMetrics = metrics
      taskData.errorMessage = errorMessage

      for (
        activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskEnd.stageId);
        jobId <- activeJobsDependentOnStage;
        jobData <- jobIdToData.get(jobId)
      ) {
        jobData.numActiveTasks -= 1
        taskEnd.reason match {
          case Success =>
            jobData.numCompletedTasks += 1
          case _ =>
            jobData.numFailedTasks += 1
        }
      }
    }
  }

  /**
    * Upon receiving new metrics for a task, updates the per-stage and per-executor-per-stage
    * aggregate metrics by calculating deltas between the currently recorded metrics and the new
    * metrics.
    */
  def updateAggregateMetrics(
                              stageData: StageUIData,
                              execId: String,
                              taskMetrics: TaskMetrics,
                              oldMetrics: Option[TaskMetrics]) {
    val execSummary = stageData.executorSummary.getOrElseUpdate(execId, new ExecutorSummary)

    val shuffleWriteDelta =
      (taskMetrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten).getOrElse(0L))
    stageData.shuffleWriteBytes += shuffleWriteDelta
    execSummary.shuffleWrite += shuffleWriteDelta

    val shuffleWriteRecordsDelta =
      (taskMetrics.shuffleWriteMetrics.map(_.shuffleRecordsWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleRecordsWritten).getOrElse(0L))
    stageData.shuffleWriteRecords += shuffleWriteRecordsDelta
    execSummary.shuffleWriteRecords += shuffleWriteRecordsDelta

    val shuffleReadDelta =
      (taskMetrics.shuffleReadMetrics.map(_.totalBytesRead).getOrElse(0L)
        - oldMetrics.flatMap(_.shuffleReadMetrics).map(_.totalBytesRead).getOrElse(0L))
    stageData.shuffleReadTotalBytes += shuffleReadDelta
    execSummary.shuffleRead += shuffleReadDelta

    val shuffleReadRecordsDelta =
      (taskMetrics.shuffleReadMetrics.map(_.recordsRead).getOrElse(0L)
        - oldMetrics.flatMap(_.shuffleReadMetrics).map(_.recordsRead).getOrElse(0L))
    stageData.shuffleReadRecords += shuffleReadRecordsDelta
    execSummary.shuffleReadRecords += shuffleReadRecordsDelta

    val inputBytesDelta =
      (taskMetrics.inputMetrics.map(_.bytesRead).getOrElse(0L)
        - oldMetrics.flatMap(_.inputMetrics).map(_.bytesRead).getOrElse(0L))
    stageData.inputBytes += inputBytesDelta
    execSummary.inputBytes += inputBytesDelta

    val inputRecordsDelta =
      (taskMetrics.inputMetrics.map(_.recordsRead).getOrElse(0L)
        - oldMetrics.flatMap(_.inputMetrics).map(_.recordsRead).getOrElse(0L))
    stageData.inputRecords += inputRecordsDelta
    execSummary.inputRecords += inputRecordsDelta

    val outputBytesDelta =
      (taskMetrics.outputMetrics.map(_.bytesWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.outputMetrics).map(_.bytesWritten).getOrElse(0L))
    stageData.outputBytes += outputBytesDelta
    execSummary.outputBytes += outputBytesDelta

    val outputRecordsDelta =
      (taskMetrics.outputMetrics.map(_.recordsWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.outputMetrics).map(_.recordsWritten).getOrElse(0L))
    stageData.outputRecords += outputRecordsDelta
    execSummary.outputRecords += outputRecordsDelta

    val diskSpillDelta =
      taskMetrics.diskBytesSpilled - oldMetrics.map(_.diskBytesSpilled).getOrElse(0L)
    stageData.diskBytesSpilled += diskSpillDelta
    execSummary.diskBytesSpilled += diskSpillDelta

    val memorySpillDelta =
      taskMetrics.memoryBytesSpilled - oldMetrics.map(_.memoryBytesSpilled).getOrElse(0L)
    stageData.memoryBytesSpilled += memorySpillDelta
    execSummary.memoryBytesSpilled += memorySpillDelta

    val timeDelta =
      taskMetrics.executorRunTime - oldMetrics.map(_.executorRunTime).getOrElse(0L)
    stageData.executorRunTime += timeDelta
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    for ((taskId, sid, sAttempt, taskMetrics) <- executorMetricsUpdate.taskMetrics) {
      val stageData = stageIdToData.getOrElseUpdate((sid, sAttempt), {
        logWarning("Metrics update for task in unknown stage " + sid)
        new StageUIData
      })
      val taskData = stageData.taskData.get(taskId)
      taskData.foreach { t =>
        if (!t.taskInfo.finished) {
          updateAggregateMetrics(stageData, executorMetricsUpdate.execId, taskMetrics,
            t.taskMetrics)

          // Overwrite task metrics
          t.taskMetrics = Some(taskMetrics)
        }
      }
    }
  }

  override def onApplicationStart(appStarted: SparkListenerApplicationStart) {
    startTime = appStarted.time
  }

  override def onApplicationEnd(appEnded: SparkListenerApplicationEnd) {
    endTime = appEnded.time
  }

  /**
    * Called when the driver registers a new executor.
    */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    executorAvailable += executorAdded.executorId
    executorIdToInfo(executorAdded.executorId) = executorAdded.executorInfo
  }


  override def onExecutorAssigned(
                                   executorAssigned: SparkListenerExecutorAssigned): Unit = synchronized {
    val stageId = executorAssigned.stageId
    execIdToStageId(executorAssigned.executorId) = stageId
    stageIdToExecId(stageId) += executorAssigned.executorId
    logInfo("Assigned %s stage to %s executor".format(
      stageId, executorAssigned.executorId))

    val jobId = stageIdToActiveJobIds(stageId)
    val controller = new ControllerJob(
      stageIdToInfo(stageId).numTasks, deadlineJobs(jobId.head), ALPHA, NOMINAL_RATE)
    controller.initControllerExecutor(
      "spark://Worker@" + executorIdToInfo(executorAssigned.executorId).executorHost + ":9999",
      executorAssigned.executorId, stageId,
      stageIdToDeadline(stageId),
      stageIdToCore(stageId))

  }
}