
package org.apache.spark.deploy.worker

/**
  * Created by Matteo on 21/07/2016.
  */
class ControllerExecutor(deadline: Long, coreMin: Int, coreMax: Int, tasks: Int, core: Int) {

  val K: Int = 1
  val Ts: Int = 2
  val Ti: Int = 3
  var csio: Long = 0
  var cs: Long = 0

  def nextAllocation(now: Long, statx: Int = 3, tasksDone: Int): Unit = {
    val csp = K * (tasks - tasksDone)
    if (statx != 3) {
      cs = coreMin
    }
    else {
      val csi = csio + K * (Ts / Ti) * (tasks - tasksDone)
      cs = math.min(coreMax, math.max(coreMin, math.ceil(csp + csi).toInt))
    }
    csio = cs - csp
  }


}
