package akka.contrib.persistence.scheduling

import akka.actor.{ ActorLogging, Actor }

object Recorder {
  case object Recording
}
class Recorder extends Actor with ActorLogging {
  import Recorder._
  var messages: List[(Long, String)] = Nil
  def now = System.currentTimeMillis()
  def receive = {
    case msg: String ⇒
      log.info(s"Recorder.receive: '$msg'")
      messages ::= now -> msg
    case Recording ⇒
      log.info(s"Recorder.receive: Recording")
      sender ! messages
  }
}
