
package org.apache.spark.deploy.master

/**
  * Created by Matteo on 22/07/2016.
  */
trait CloudProvider {

  def create_vm(masterUrl: String)

  def delete_vm(host: String)

}
