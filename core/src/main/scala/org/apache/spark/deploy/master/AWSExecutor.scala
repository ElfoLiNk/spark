
package org.apache.spark.deploy.master

/**
  * Created by Matteo on 22/07/2016.
  */
class AWSExecutor extends CloudExecutor {
  val awsProvider = new AWSProvider()

  override def create_vms(new_vms: Int, masterUrl: String): Unit = {
    for (i <- 0 until new_vms){
      awsProvider.create_vm(masterUrl)
    }

  }

  override def delete_vms(remove_vms: List[WorkerInfo]): Unit = {

    remove_vms.foreach(vm => awsProvider.delete_vm(vm.host))
  }
}
