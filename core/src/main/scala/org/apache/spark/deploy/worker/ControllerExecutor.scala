
package org.apache.spark.deploy.worker

/**
  * Created by Matteo on 21/07/2016.
  */
class ControllerExecutor(deadline: Long, coreMin: Int, coreMax: Int, _tasks: Int, core: Int) {

  val K: Int = 50
  val Ts: Int = 1
  // Sampling Time
  val Ti: Int = 10
  val CQ: Double = 0.5
  // Cores Quantum
  val tasks: Int = _tasks
  var worker: Worker = _

  var csiOld: Double = 0.0
  var SP: Double = 0.0
  var completedTasks: Double = 0.001
  var cs: Double = 0.0

  val t = new java.util.Timer()
  val spgen = new java.util.TimerTask {
    def run() = SP += Ts / deadline
    nextAllocation()
  }
  t.schedule(spgen, Ts, Ts)
  spgen.cancel()

  def nextAllocation(statx: Int = 3): Int = {
    val csp = K * (SP - tasks / completedTasks)
    if (statx != 3) {
      cs = coreMin
    }
    else {
      val csi = csiOld + K * (Ts / Ti) * (SP - tasks / completedTasks)
      cs = math.min(coreMax, math.max(coreMin, math.ceil(csp + csi)))
    }
    cs = math.ceil(cs / CQ) * CQ
    csiOld = cs - csp
    math.ceil(cs).toInt
  }

}
