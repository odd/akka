/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.cluster.routing.random.replicationfactor_1

import akka.cluster._
import akka.cluster.Cluster._
import akka.actor._
import akka.config.Config

/**
 * Test that if a single node is used with a random router with replication factor then the actor is instantiated
 * on the single node.
 */
object Random1ReplicaMultiJvmSpec {

  class HelloWorld extends Actor with Serializable {
    def receive = {
      case "Hello" ⇒
        self.reply("World from node [" + Config.nodename + "]")
    }
  }

}

class Random1ReplicaMultiJvmNode1 extends MasterClusterTestNode {

  import Random1ReplicaMultiJvmSpec._

  val testNodes = 1

  "Random Router: A cluster" must {

    "create clustered actor, get a 'local' actor on 'home' node and a 'ref' to actor on remote node" in {
      node.start()

      var hello = Actor.actorOf[HelloWorld]("service-hello")
      hello must not equal (null)
      hello.address must equal("service-hello")
      hello.isInstanceOf[ClusterActorRef] must be(true)

      hello must not equal (null)
      val reply = (hello ? "Hello").as[String].getOrElse(fail("Should have recieved reply from node1"))
      reply must equal("World from node [node1]")

      node.shutdown()
    }
  }
}
