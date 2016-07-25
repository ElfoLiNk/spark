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

import org.apache.spark.deploy.master.Master
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{InitControllerExecutor, NeededCore}

class ControllerJob
(tasks: Int, deadlineJob: Long, alpha: Double, nominalRate: Double) extends Logging {

  val alphaDeadline: Long = (alpha * deadlineJob.toDouble).toLong
  val memForCore: Double = 2048000.0
  val coreForVM: Int = 8
  val numMaxExecutor: Int = 4

  var numExecutor = 0
  var coreForExecutor = new scala.collection.mutable.HashMap[Int, Int]


  val conf = new SparkConf
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = RpcEnv.create("ControllEnv", "localhost", 6666, conf, securityMgr, clientMode = true)
  val controllerEndpoint = rpcEnv.setupEndpoint("ControllJob",
    new ControllerJob(rpcEnv, "ControllEnv", "ControllJob", conf, securityMgr))
  // rpcEnv.awaitTermination()


  def stop(): Unit = {
    rpcEnv.stop(controllerEndpoint)
  }

  def computeDeadlineStage(stage: StageInfo, weight: Long): Long = {
    if (weight != 0) {
      (alphaDeadline - stage.submissionTime.get) / weight
    }
    alphaDeadline
  }

  def computeCoreStage(deadlineStage: Long, numRecord: Long): Int = {
    logInfo("NumRecords: " + numRecord.toString +
      " DeadlineStage : " + deadlineStage.toString + " NominalRate: " + nominalRate.toString)
    math.ceil(numRecord / deadlineStage / nominalRate).toInt
  }

  def computeDeadlineFirstStage(stage: StageInfo, weight: Long): Long = {
    if (weight != 0) {
      (alphaDeadline - stage.submissionTime.get) / weight
    }
    alphaDeadline
  }

  def computeCoreFirstStage(stage: StageInfo): Int = {
    val totalSize = stage.rddInfos.foldLeft(0L) {
      (acc, rdd) => acc + rdd.memSize + rdd.diskSize + rdd.externalBlockStoreSize
    }
    logInfo("Size First Stage: " + totalSize.toString)
    math.ceil(totalSize * 10 / memForCore).toInt
  }

  def computeNumExecutorAndTaskToEach(coresToBeAllocated: Int): Unit = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt

    val coresPerExecutor = (1 to numExecutor).map {
      i => if (coresToBeAllocated % numExecutor >= i) {
        1 + (coresToBeAllocated / numExecutor)
      } else coresToBeAllocated / numExecutor
    }

    val taskPerExecutor = scala.collection.mutable.IndexedSeq((0 until numExecutor).map {
      tasks * coresPerExecutor(_) / coresToBeAllocated
    }: _*)

    val remainingTasks = tasks - taskPerExecutor.sum

    (0 until remainingTasks).foreach { i =>
      taskPerExecutor(i % numExecutor) = taskPerExecutor(i % numExecutor) + 1
    }

    taskPerExecutor
  }


  def initControllerExecutor(
    workerUrl: String, executorId: String, stageId: Long, deadline: Long, core: Int): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(InitControllerExecutor(executorId, stageId, tasks, deadline, core))
    logInfo("SEND INIT TO EXECUTOR CONTROLLER %s, %s, %s, %s, %s".format
    (executorId, stageId, tasks, deadline, core))
  }

  def askMasterNeededCore
  (masterUrl: String, stageId: Long, coreNeeded: Int, appname: String): Unit = {
    val masterRef = rpcEnv.setupEndpointRef(
      Master.SYSTEM_NAME, RpcAddress.fromSparkURL(masterUrl), Master.ENDPOINT_NAME)
    masterRef.send(NeededCore(stageId, coreNeeded, appname))
    logInfo("SEND NEEDED CORE TO MASTER %s, %s, %s, %s, %s".format
    (masterUrl, stageId, tasks, coreNeeded, appname))

  }

  class ControllerJob(
                       override val rpcEnv: RpcEnv,
                       systemName: String,
                       endpointName: String,
                       val conf: SparkConf,
                       val securityMgr: SecurityManager)
    extends ThreadSafeRpcEndpoint with Logging {
  }

}


