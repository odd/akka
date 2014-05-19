/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.persistence.scheduling.cron

import akka.contrib.persistence.scheduling.Schedule
import scala.concurrent.duration._
import akka.contrib.persistence.scheduling.internal.scalendar.Scalendar

case class Cron(expression: String) extends Schedule {
  import akka.contrib.persistence.scheduling.internal.cronish.dsl._
  @transient private[this] lazy val cron = expression.cron
  def next(timestamp: Long = System.currentTimeMillis()) = Option(timestamp + cron.nextFrom(Scalendar(timestamp)))
}