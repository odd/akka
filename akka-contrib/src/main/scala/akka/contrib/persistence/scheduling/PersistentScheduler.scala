/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.persistence.scheduling

import scala.concurrent.duration.{ Duration, FiniteDuration }
import akka.pattern.ask
import akka.actor._
import scala.concurrent.{ Future, Await, ExecutionContext }
import scala.util.control.NoStackTrace
import java.util.{ Calendar, TimeZone, GregorianCalendar }
import scala.util.matching.Regex
import akka.persistence._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{ AtomicLong, AtomicBoolean }
import akka.util.Timeout
import scala.concurrent.duration._
import scala.Some
import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.persistence.SnapshotOffer
import akka.contrib.persistence.scheduling.PersistentScheduler.PersistentCancellable
import akka.contrib.persistence.versioning.{Version, Versioned}

case class PersistentSchedulerException(msg: String) extends akka.AkkaException(msg) with NoStackTrace

object PersistentSchedulerExtension extends ExtensionId[PersistentSchedulerExtension] with ExtensionIdProvider {
  def lookup = this
  def createExtension(s: ExtendedActorSystem) = new PersistentSchedulerExtension(s)
}

class PersistentSchedulerExtension(val system: ExtendedActorSystem) extends Extension {
  val config = system.settings.config.atPath("akka.contrib.persistence.scheduling")
  implicit val defaultTimeout = Timeout(if (config.hasPath("defaultTimeout")) config.getLong("defaultTimeout") else 3000L, SECONDS)
  val snapshotInterval = if (config.hasPath("snapshotInterval")) config.getLong("snapshotInterval") else 1000L
  val scheduler = {
    implicit val sys = system
    new DefaultPersistentScheduler(snapshotInterval = Duration(snapshotInterval, TimeUnit.MILLISECONDS))
  }
}

/**
 * A persistent Akka scheduler service.
 */
trait PersistentScheduler {
  import PersistentScheduler._
  import Schedule._

  /**
   * Schedules a message to be sent repeatedly with an initial delay and
   * frequency. E.g. if you would like a message to be sent immediately and
   * thereafter every 500ms you would set delay=Duration.Zero and
   * interval=Duration(500, TimeUnit.MILLISECONDS)
   *
   * Java & Scala API
   */
  def schedule(
    schedule: Schedule,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  timeout: Timeout,
                  sender: ActorRef = Actor.noSender): PersistentCancellable

  def scheduleOnce(
    schedule: Singular,
    receiver: ActorRef,
    message: Any)(implicit executor: ExecutionContext,
                  timeout: Timeout,
                  sender: ActorRef = Actor.noSender): PersistentCancellable

  /**
   * The maximum supported task frequency of this scheduler, i.e. the inverse
   * of the minimum time interval between executions of a recurring task, in Hz.
   */
  def maxFrequency: Double

  def cancel(id: Long): Unit

  def list: Future[Seq[(Long, Schedule)]]

  def system: ActorSystem

  def timeout: Timeout
}
object PersistentScheduler {
  trait PersistentCancellable extends Cancellable {
    def id: Future[Long]
  }
}

// this one is just here so we can present a nice AbstractPersistentScheduler for Java
abstract class AbstractPersistentSchedulerBase extends PersistentScheduler

trait Schedule {
  def next(timestamp: Long = System.currentTimeMillis()): Option[Long]
}
object Schedule {
  trait Singular extends Schedule {
    val time: Long
    final def next(timestamp: Long = System.currentTimeMillis()): Option[Long] = {
      println("## time >= (timestamp - resolution.toMillis): " + s"$time >= $timestamp ==> " + (time >= timestamp))
      if (time >= timestamp) Some(time)
      else None
    }
  }
}
object Schedules {
  import Schedule._
  case class Every(interval: FiniteDuration, first: Long = System.currentTimeMillis()) extends Schedule {
    def next(timestamp: Long = System.currentTimeMillis()): Some[Long] = {
      if (timestamp <= first) Some(first)
      else {
        val length = interval.toMillis
        val time = timestamp + (length - (timestamp - first) % length)
        Some(time)
      }
    }
  }
  case class At(time: Long) extends Singular
  object In {
    def apply(delay: Long) = At(System.currentTimeMillis() + delay)
  }
  object Timestamp {
    val Iso8601: Regex = """^(\d{4,})-(\d{2})-(\d{2})(?:T(\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{3}))?)?)?)?([+-]\d{1,2}:\d{2})?$""".r
    def apply(timestamp: String): Singular = timestamp match {
      case Iso8601(year, month, day, hour, minute, second, millis, timeZone) ⇒
        def int(str: String) = if (str == null || str == "") 0 else str.toInt
        val calendar = new GregorianCalendar(int(year), int(month) - 1, int(day), int(hour), int(minute), int(second))
        if (timeZone != null && timeZone != "") calendar.setTimeZone(TimeZone.getTimeZone("GMT" + timeZone))
        calendar.set(Calendar.MILLISECOND, int(millis))
        At(calendar.getTimeInMillis)
      case _ ⇒
        sys.error("Unknown timestamp schedule format (expected ISO 8601: yyyy-MM-ddTHH:mm:ss.SSS±01:00): " + timestamp)
    }
  }
}

class DefaultPersistentScheduler(snapshotInterval: FiniteDuration = 1.minute, clear: Boolean = false)(implicit val system: ActorSystem, val timeout: Timeout) extends PersistentScheduler {
  import Schedule._
  import Scheduler._

  private[this] val scheduler: ActorRef = {
    val i = snapshotInterval
    system.actorOf(Props(new Scheduler(i)))
  }
  if (clear) scheduler ! Clear

  /**
   * The maximum supported task frequency of this scheduler, i.e. the inverse
   * of the minimum time interval between executions of a recurring task, in Hz.
   */
  def maxFrequency = 1000L

  def scheduleOnce(s: Singular, receiver: ActorRef, message: Any)(implicit executor: ExecutionContext = system.dispatcher, timeout: Timeout = timeout, sender: ActorRef = Actor.noSender) =
    schedule(s, receiver, message)

  def schedule(s: Schedule, receiver: ActorRef, message: Any)(implicit executor: ExecutionContext = system.dispatcher, timeout: Timeout = timeout, sender: ActorRef = Actor.noSender): PersistentCancellable = {
    val f = (scheduler ? Register(s, receiver, message, sender)).mapTo[Long]
    val initial = -2
    val pending = -1
    val state = new AtomicLong(initial)
    f.onSuccess {
      case id ⇒ state.set(id)
    }
    println(s"[${System.currentTimeMillis()}] sent register")
    new PersistentCancellable {
      def cancel(): Boolean = {
        if (state.get() == pending) false
        else {
          state.set(pending)
          f.onSuccess {
            case id ⇒
              scheduler ! Cancel(id)
              state.set(id)
          }
          true
        }
      }

      def isCancelled: Boolean = state.get != initial

      override val id = f
    }
  }

  override def cancel(id: Long) = scheduler ! Cancel(id)

  override def list = (scheduler ? List).mapTo[Seq[(Long, Schedule)]]
}

private[scheduling] object Scheduler {
  type Id = Long
  trait Command
  trait Event {
    def id: Long
  }
  case class Register(schedule: Schedule, receiver: ActorRef, message: Any, sender: ActorRef) extends Command
  case class Cancel(id: Id) extends Command
  case object List extends Command
  case object Clear extends Command

  case class Registration(id: Id, schedule: Schedule, receiverPath: ActorPath, message: Any, senderPath: ActorPath) extends Event {
    def receiver(implicit system: ActorSystem, timeout: Timeout = Timeout(5.seconds)): ActorRef = actorRef(receiverPath)
    def sender(implicit system: ActorSystem, timeout: Timeout = Timeout(5.seconds)): ActorRef = actorRef(senderPath)
    def active = schedule.next().map(_ > now).getOrElse(false)
  }
  case class Cancelled(id: Id) extends Event
  case class State(maxId: Long = 0L, schedules: Map[Id, Registration] = Map.empty) {
    def update(r: Registration) = copy(maxId = math.max(maxId, r.id), schedules = if (r.active) schedules + (r.id -> r) else schedules)
    def update(c: Cancelled) = copy(maxId = math.max(maxId, c.id), schedules = schedules - c.id)
    def nextId() = maxId + 1
    override def toString: String = schedules.mkString("Schedules (max id: " + maxId + "):\n\t", "\n\t", "\n")
  }
  object Active {
    def unapply(r: Registration)(implicit timeout: Timeout = Timeout(1.second)): Option[(Registration, Long)] = r.schedule.next() match {
      case Some(t) if (t > (now - timeout.duration.toMillis)) ⇒
        debug(s"Active.unapply some: $r, $t")
        Some((r, t))
      case x ⇒
        debug(s"Active.unapply none: $r, $x")
        None
    }
  }

  case object Snapshoot extends Command
  case class Invoke(registration: Registration) extends Command

  private def now = System.currentTimeMillis()
  var startTime = System.currentTimeMillis()
  private def debug(msg: String): Unit = if (true) println(s"[$now / ${now - startTime}] $msg")
  private def actorRef(path: ActorPath)(implicit system: ActorSystem, timeout: Timeout = Timeout(5.seconds)): ActorRef = {
    import akka.pattern.ask
    if (path == null) ActorRef.noSender
    else {
      val selection = system.actorSelection(path)
      val id = selection.toString
      Await.result((selection ? Identify(id)).mapTo[ActorIdentity], timeout.duration) match {
        case ActorIdentity(`id`, Some(ref)) ⇒ ref
        case _                              ⇒ ActorRef.noSender
      }
    }
  }
}
private[scheduling] class Scheduler(snapshotInterval: FiniteDuration) extends EventsourcedProcessor with ActorLogging {
  import Scheduler._
  implicit val system = context.system
  import system.dispatcher

  private[this] var state = State()
  private[this] var cancellables = Map.empty[Long, Cancellable]

  override def preStart() = {
    super.preStart()
    cancellables += 0L -> system.scheduler.schedule(snapshotInterval, snapshotInterval, self, Snapshoot)
  }

  override def postStop() = {
    cancellables.foreach(_._2.cancel())
    super.postStop()
  }

  def update(e: Event): Unit = e match {
    case a @ Active(r: Registration, t: Long) if (r.receiver != null) ⇒
      debug(s"update active: $a")
      val delay = t - now
      debug(s"update: scheduling: $r with delay $delay and sender ${r.sender}")
      cancellables += r.id -> system.scheduler.scheduleOnce(Duration(delay, TimeUnit.MILLISECONDS), self, Invoke(r))(system.dispatcher, r.sender)
      debug(s"update: scheduling done")
      state = state.update(r)
    case r: Registration ⇒
      debug(s"update: $r")
      state = state.update(r)
    case c @ Cancelled(id) ⇒
      debug(s"update: $c")
      cancellables.get(id).foreach(_.cancel())
      cancellables -= id
      state = state.update(c)
  }

  val receiveRecover: Receive = {
    case e: Event ⇒
      debug(s"receiveRecover with event: $e")
      update(e)
    case so @ SnapshotOffer(_, snapshot: State) ⇒
      debug(s"receiveRecover with snapshot: $so")
      snapshot.schedules.values.foreach(update)
  }

  val receiveCommand: Receive = {
    case Snapshoot ⇒
      debug(s"receiveCommand: Snapshoot")
      saveSnapshot(state)
    case sss @ SaveSnapshotSuccess(metadata) ⇒
      debug(s"receiveCommand: $sss")
      deleteMessages(toSequenceNr = metadata.sequenceNr - 1, permanent = true)
      deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = metadata.sequenceNr - 1, maxTimestamp = metadata.timestamp - 1))
    case SaveSnapshotFailure(metadata, reason) ⇒
      log.error(reason, "Unable to save snapshot [metadata: {}]", metadata)
    case l @ List ⇒
      debug(s"receiveCommand: $l")
      sender ! state.schedules
    case c @ Clear ⇒
      debug(s"receiveCommand: $c")
      cancellables.foreach(_._2.cancel())
      state = State()
      deleteMessages(toSequenceNr = lastSequenceNr, permanent = true)
      deleteSnapshots(SnapshotSelectionCriteria(maxSequenceNr = lastSequenceNr))
    case i @ Invoke(r) ⇒
      debug(s"receiveCommand: $i")
      r.receiver ! r.message
      cancellables -= r.id
      update(r)
    case r @ Register(scheduling, receiver, message, messageSender) ⇒
      debug(s"receiveCommand: $r")
      val registration = Registration(state.nextId(), scheduling, receiver.path, message, Option(messageSender).map(_.path).orNull)
      persist(registration)(update)
    case c @ Cancel(id) ⇒
      debug(s"receiveCommand: $c")
      persist(Cancelled(id))(update)
  }
}
