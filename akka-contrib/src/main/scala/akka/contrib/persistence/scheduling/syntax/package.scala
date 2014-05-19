package akka.contrib.persistence.scheduling

import scala.concurrent.ExecutionContext
import akka.actor.ActorRef
import akka.util.Timeout
import akka.contrib.persistence.scheduling.{ Schedule, PersistentScheduler }
import akka.contrib.persistence.scheduling.PersistentScheduler.PersistentCancellable

package object syntax {
  implicit class SchedulingActorRef(target: ActorRef)(implicit scheduler: PersistentScheduler) {
    def !@(messageWithSchedule: Pair[Any, Schedule])(implicit executor: ExecutionContext = scheduler.system.dispatcher,
                                                     timeout: Timeout = scheduler.timeout,
                                                     sender: ActorRef = ActorRef.noSender): PersistentCancellable = {
      scheduler.schedule(
        schedule = messageWithSchedule._2,
        receiver = target,
        message = messageWithSchedule._1)
    }
    def tellAt(message: Any, schedule: Schedule): PersistentCancellable = {
      this.!@((message, schedule))
    }
    def tellAt(message: Any, schedule: Schedule, sender: ActorRef): PersistentCancellable = this.!@((message, schedule))(sender = sender)
  }
}
