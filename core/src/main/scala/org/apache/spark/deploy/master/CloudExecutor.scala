
package org.apache.spark.deploy.master

/**
  * Created by Matteo on 21/07/2016.
  */
trait CloudExecutor {

  def create_vms(new_vms: Int, masterUrl: String)

  def delete_vms(remove_vms: List[WorkerInfo])
}
