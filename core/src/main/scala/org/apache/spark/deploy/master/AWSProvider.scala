
package org.apache.spark.deploy.master

import awscala._
import ec2._
import com.amazonaws.services.ec2.model.InstanceType

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.collection._

import org.apache.spark.Logging

/**
  * Created by Matteo on 22/07/2016.
  */
class AWSProvider extends CloudProvider with Logging {

  var region: Region = Region.Oregon
  var flavor: InstanceType = InstanceType.M44xlarge
  var ami: String = "ami-2dae215e"

  val filename = "conf/aws.properties"
  for (line <- Source.fromFile(filename).getLines()) {
    val split = line.split("=")
    if (split.length == 2) {
      System.setProperty(split(0), split(1))
    }

  }

  implicit val ec2 = EC2.at(region)

  def create_vm(masterUrl: String): Unit = {
    val f = Future(ec2.runAndAwait(ami, ec2.keyPair("gazzetta").get, flavor))
    for {
      instances <- f
      instance <- instances
    } {
      // scalastyle:off line.size.limit
      instance.withKeyPair(new java.io.File("key_pair_file")) { i =>
        i.ssh { ssh =>
          ssh.exec("/usr/local/spark/sbin/start-slave.sh %s --port 9999 && sudo ./disable-ht.sh".format(masterUrl)).right.map {
            result => logInfo(s"IstanceId: ${i.instanceId} Result: " + result.stdOutAsString())
          }
        }
      }
      // scalastyle:on line.size.limit
    }
  }

  def delete_vm(host: String): Unit = {
    ec2.instances.filter(i => i.privateDnsName == host).foreach(i => i.terminate())
  }
}
