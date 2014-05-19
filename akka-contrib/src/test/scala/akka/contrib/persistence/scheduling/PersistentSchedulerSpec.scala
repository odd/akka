/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.persistence.scheduling

import org.scalatest.{ Matchers, BeforeAndAfterAll, FunSuiteLike }
import akka.actor.{ Actor, Props, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import akka.contrib.persistence.scheduling.Schedules._
import scala.concurrent.duration._
import akka.util.Timeout
import java.util.concurrent.TimeUnit

object PersistentSchedulerSpec {

  class EchoActor extends Actor {
    def receive = {
      case x ⇒ sender ! x
    }
  }

}

class PersistentSchedulerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FunSuiteLike with Matchers with BeforeAndAfterAll {

  import PersistentSchedulerSpec._

  implicit val timeout: Timeout = 5.seconds

  def this() = this(ActorSystem("PersistentSchedulerSpec"))

  override protected def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  test("Send back messages unchanged") {
    val echo = system.actorOf(Props[EchoActor])
    echo ! "hello world"
    expectMsg("hello world")
  }

  test("Scheduling should work") {
    import system.dispatcher
    val echo = system.actorOf(Props[EchoActor])
    val pe = new DefaultPersistentScheduler(clear = true)
    pe.scheduleOnce(At(System.currentTimeMillis() + 2000L), echo, "Ping")
    println(s"[${System.currentTimeMillis()}] waiting...")
    expectMsg(50.seconds, "Ping")
    println(s"[${System.currentTimeMillis()}] done")
  }

  test("Syntax should work") {
    import akka.contrib.persistence.scheduling.syntax._
    implicit val scheduler = PersistentSchedulerExtension(system).scheduler

    val echo = system.actorOf(Props[EchoActor])
    echo !@ "hello world" -> In(1000L)
    expectMsg("hello world")

  }

  /*
  test("Scheduling should survive actor system restart") {
    import system.dispatcher
    val echo = system.actorOf(Props[EchoActor])
    val pe = new DefaultPersistentScheduler
    pe.schedule(At(1.second.fromNow.time.toMillis), echo, s"Ping")
    Thread.sleep(2000L)
    println("shutting down")
    system.shutdown()
  }
  */

  /*
  test("Scheduling should survive actor system restart") {
    val id = now % 1000L
    val time = now + 10000L
    val interval = 10L.seconds
    implicit val timeout = Timeout(2000L)

    {
      implicit val system = ActorSystem("PersistentSchedulerSpec")
      import system.dispatcher
      val pe = new DefaultPersistentScheduler
      val recorder = system.actorOf(Props[Recorder], "recorder")
      pe.schedule(Every(interval), recorder, s"Ping from ${id}")
      Thread.sleep(1000L)
      println("shutting down")
      system.shutdown()
    }

    Thread.sleep(2000L)

    {
      implicit val system = ActorSystem("PersistentSchedulerSpec")
      import system.dispatcher
      val pe = new DefaultPersistentScheduler
      val recorder = system.actorOf(Props[Recorder], "recorder")
      Thread.sleep(10000L)
      val f = (recorder ? Recorder.Recording).mapTo[List[(Long, String)]]
      f.collect {
        case xs: List[String] ⇒
          println(xs)
        //assert(t >= time, s"timing off: expected time greater than or equal to $time but got $t")
        case x ⇒
          println("unknown state: " + x)
      }
      Console.readLine()
      system.shutdown()
    }
  }*/
}