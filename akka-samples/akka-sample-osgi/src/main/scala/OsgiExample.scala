/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */
package sample.osgi

import akka.actor.Actor
import akka.actor.Actor._

import org.osgi.framework.{ BundleActivator, BundleContext }

class Activator extends BundleActivator {

  def start(context: BundleContext) {
    println("Starting the OSGi example ...")
    val echo = actorOf[EchoActor].start()
    val answer = (echo ? "OSGi example").as[String]
    println(answer getOrElse "No answer!")
  }

  def stop(context: BundleContext) {
    Actor.registry.local.shutdownAll()
    println("Stopped the OSGi example.")
  }
}

class EchoActor extends Actor {

  override def receive = {
    case x => self.reply(x)
  }
}
