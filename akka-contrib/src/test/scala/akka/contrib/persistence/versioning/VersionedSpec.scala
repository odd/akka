package akka.contrib.persistence.versioning

import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import akka.persistence.{ SnapshotOffer, EventsourcedProcessor }
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import akka.testkit.{ TestKit, ImplicitSender }

//import akka.testkit.{AkkaSpec, ImplicitSender}
import akka.actor.{ Props, ActorSystem }
import akka.pattern._

object VersionedSpec {

  trait Name extends Versioned[Name]

  case class Name1(first: String, last: String) extends Name

  case class Name2(first: String, last: String, title: Option[String] = None) extends Name

  object Name extends Versioned.Companion[Name] {
    override def apply(data: (Version, Product)): Name = data match {
      case (_, (first: String, last: String))                        ⇒ Name2(first, last, Some("N/A"))
      case (_, (first: String, last: String, title: Option[String])) ⇒ Name2(first, last, title)
    }

    override def unapply(t: Name): Option[Product] = t match {
      case Name1(first, last)        ⇒ Some((first, last))
      case Name2(first, last, title) ⇒ Some((first, last, title))
      case _                         ⇒ None
    }
  }

  object LoggingActor {

    trait Command

    trait Event

    case object List extends Command

    case object Clear extends Command

    case class Logged(timestamp: Long, message: Any) extends Event

    case class Cleared(timestamp: Long) extends Event

    case class State(entries: Seq[Logged] = Seq.empty)

    def now = System.currentTimeMillis()
  }

  import LoggingActor._

  class LoggingActor extends EventsourcedProcessor {
    var state: State = State()

    def update(e: Event): Unit = e match {
      case c: Cleared ⇒
        state = State()
      case l: Logged ⇒
        state = state.copy(entries = l +: state.entries)
    }

    override def receiveCommand = {
      case List ⇒
        sender ! state.entries.map(_.message).reverse
      case Clear ⇒
        persist(Cleared(timestamp = now))(update)
      case msg ⇒
        persist(Logged(now, msg))(update)
    }

    override def receiveRecover = {
      case s @ State(entries) ⇒
        state = s
      case SnapshotOffer(_, s: State) ⇒
        state = s
    }
  }
}

/*
class VersionedSpec extends AkkaSpec("""
    akka {
      loglevel = "DEBUG"
      actor {
        debug {
          receive = on
          autoreceive = on
          lifecycle = on
        }
        serializers {
          versioned = "akka.contrib.persistence.versioning.VersionedSerializer"
        }
        serialization-bindings {
          "akka.contrib.persistence.versioning.Versioned" = versioned
        }
      }
      persistence.journal.leveldb.native = off
    }
    """) with ImplicitSender {

  import VersionedSpec._
  import LoggingActor._

  "A versioned message" must {
    "be serialized as version-product pair" in {
      val logger = system.actorOf(Props[LoggingActor], "logger")
      logger ! Clear
      logger ! Name1("Odd", "Möller")
      logger ! Name1("Kalle", "Karlsson")
      //system1.shutdown()

      logger ! List
      expectMsg(scala.List(Name1("Odd", "Möller"), Name1("Kalle", "Karlsson")))

      logger ! Name2("Nisse", "Nilsson", Some("PhD"))
      logger ! List
      expectMsg(scala.List(Name1("Odd", "Möller"), Name1("Kalle", "Karlsson"), Name2("Nisse", "Nilsson", Some("PhD"))))
    }
  }
}
*/