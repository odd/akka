/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.persistence.scheduling
package cron

import org.scalatest.FunSuiteLike
import akka.actor.{ ActorLogging, Props, Actor, ActorSystem }
import akka.pattern.ask
import scala.concurrent.duration._
import akka.util.Timeout
import java.util.concurrent.TimeUnit

class CronScheduleSpec extends FunSuiteLike {
  test("Cron expressions") {
    implicit val timeout = Timeout(2000L, TimeUnit.SECONDS)
    implicit val system = ActorSystem("PersistentSchedulerSpec")
    import system.dispatcher
    val pe = new DefaultPersistentScheduler
    val recorder = system.actorOf(Props[Recorder], "recorder2")
    val schedule: Cron = Cron("Every 10 seconds")
    pe.schedule(schedule, recorder, s"Ping")
    Console.readLine()
    system.shutdown()
  }
}