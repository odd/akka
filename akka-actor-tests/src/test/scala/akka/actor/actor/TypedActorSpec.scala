package akka.actor

/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

import org.scalatest.matchers.MustMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{ BeforeAndAfterAll, WordSpec, BeforeAndAfterEach }
import akka.actor.TypedActor._
import akka.japi.{ Option ⇒ JOption }
import akka.util.Duration
import akka.dispatch.{ Dispatchers, Future, KeptPromise }
import akka.routing.CyclicIterator
import java.io.{ ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream }
import akka.actor.TypedActorSpec.Foo

object TypedActorSpec {
  trait Foo {
    def pigdog(): String

    def self = TypedActor.self[Foo]

    def futurePigdog(): Future[String]
    def futurePigdog(delay: Long): Future[String]
    def futurePigdog(delay: Long, numbered: Int): Future[String]
    def futureComposePigdogFrom(foo: Foo): Future[String]

    def failingFuturePigdog(): Future[String] = throw new IllegalStateException("expected")
    def failingOptionPigdog(): Option[String] = throw new IllegalStateException("expected")
    def failingJOptionPigdog(): JOption[String] = throw new IllegalStateException("expected")

    def failingPigdog(): Unit = throw new IllegalStateException("expected")

    def optionPigdog(): Option[String]
    def optionPigdog(delay: Long): Option[String]
    def joptionPigdog(delay: Long): JOption[String]

    def incr()
    def read(): Int
  }

  class Bar extends Foo with Serializable {

    def pigdog = "Pigdog"

    def futurePigdog(): Future[String] = new KeptPromise(Right(pigdog))
    def futurePigdog(delay: Long): Future[String] = {
      Thread.sleep(delay)
      futurePigdog
    }

    def futurePigdog(delay: Long, numbered: Int): Future[String] = {
      Thread.sleep(delay)
      new KeptPromise(Right(pigdog + numbered))
    }

    def futureComposePigdogFrom(foo: Foo): Future[String] =
      foo.futurePigdog(500).map(_.toUpperCase)

    def optionPigdog(): Option[String] = Some(pigdog)

    def optionPigdog(delay: Long): Option[String] = {
      Thread.sleep(delay)
      Some(pigdog)
    }

    def joptionPigdog(delay: Long): JOption[String] = {
      Thread.sleep(delay)
      JOption.some(pigdog)
    }

    var internalNumber = 0

    def incr() {
      internalNumber += 1
    }

    def read() = internalNumber
  }

  trait Stackable1 {
    def stackable1: String = "foo"
  }

  trait Stackable2 {
    def stackable2: String = "bar"
  }

  trait Stacked extends Stackable1 with Stackable2 {
    def stacked: String = stackable1 + stackable2
    def notOverriddenStacked: String = stackable1 + stackable2
  }

  class StackedImpl extends Stacked {
    override def stacked: String = "FOOBAR" //Uppercase
  }

}

@RunWith(classOf[JUnitRunner])
class TypedActorSpec extends WordSpec with MustMatchers with BeforeAndAfterEach with BeforeAndAfterAll {

  import akka.actor.TypedActorSpec._

  def newFooBar: Foo = newFooBar(Duration(2, "s"))

  def newFooBar(timeout: Duration): Foo =
    newFooBar(Configuration(timeout))

  def newFooBar(config: Configuration): Foo =
    typedActorOf(classOf[Foo], classOf[Bar], config)

  def newStacked(config: Configuration = Configuration(Duration(2, "s"))): Stacked =
    typedActorOf(classOf[Stacked], classOf[StackedImpl], config)

  def mustStop(typedActor: AnyRef) = stop(typedActor) must be(true)

  "TypedActors" must {

    "be able to instantiate" in {
      val t = newFooBar
      isTypedActor(t) must be(true)
      mustStop(t)
    }

    "be able to stop" in {
      val t = newFooBar
      mustStop(t)
    }

    "not stop non-started ones" in {
      stop(null) must be(false)
    }

    "throw an IllegalStateExcpetion when TypedActor.self is called in the wrong scope" in {
      (intercept[IllegalStateException] {
        TypedActor.self[Foo]
      }).getMessage must equal("Calling TypedActor.self outside of a TypedActor implementation method!")
    }

    "have access to itself when executing a method call" in {
      val t = newFooBar
      t.self must be(t)
      mustStop(t)
    }

    "be able to call toString" in {
      val t = newFooBar
      t.toString must be(getActorRefFor(t).toString)
      mustStop(t)
    }

    "be able to call equals" in {
      val t = newFooBar
      t must equal(t)
      t must not equal (null)
      mustStop(t)
    }

    "be able to call hashCode" in {
      val t = newFooBar
      t.hashCode must be(getActorRefFor(t).hashCode)
      mustStop(t)
    }

    "be able to call user-defined void-methods" in {
      val t = newFooBar
      t.incr()
      t.read() must be(1)
      t.incr()
      t.read() must be(2)
      t.read() must be(2)
      mustStop(t)
    }

    "be able to call normally returning methods" in {
      val t = newFooBar
      t.pigdog() must be("Pigdog")
      mustStop(t)
    }

    "be able to call Future-returning methods non-blockingly" in {
      val t = newFooBar
      val f = t.futurePigdog(200)
      f.isCompleted must be(false)
      f.get must be("Pigdog")
      mustStop(t)
    }

    "be able to call multiple Future-returning methods non-blockingly" in {
      val t = newFooBar
      val futures = for (i ← 1 to 20) yield (i, t.futurePigdog(20, i))
      for ((i, f) ← futures) {
        f.get must be("Pigdog" + i)
      }
      mustStop(t)
    }

    "be able to call methods returning Java Options" in {
      val t = newFooBar(Duration(500, "ms"))
      t.joptionPigdog(200).get must be("Pigdog")
      t.joptionPigdog(700) must be(JOption.none[String])
      mustStop(t)
    }

    "be able to call methods returning Scala Options" in {
      val t = newFooBar(Duration(500, "ms"))
      t.optionPigdog(200).get must be("Pigdog")
      t.optionPigdog(700) must be(None)
      mustStop(t)
    }

    "be able to compose futures without blocking" in {
      val t, t2 = newFooBar(Duration(2, "s"))
      val f = t.futureComposePigdogFrom(t2)
      f.isCompleted must be(false)
      f.get must equal("PIGDOG")
      mustStop(t)
      mustStop(t2)
    }

    "be able to handle exceptions when calling methods" in {
      val t = newFooBar

      t.incr()
      t.failingPigdog()
      t.read() must be(1) //Make sure state is not reset after failure

      t.failingFuturePigdog.await.exception.get.getMessage must be("expected")
      t.read() must be(1) //Make sure state is not reset after failure

      (intercept[IllegalStateException] {
        t.failingJOptionPigdog
      }).getMessage must be("expected")
      t.read() must be(1) //Make sure state is not reset after failure

      (intercept[IllegalStateException] {
        t.failingOptionPigdog
      }).getMessage must be("expected")

      t.read() must be(1) //Make sure state is not reset after failure

      mustStop(t)
    }

    "be able to support stacked traits for the interface part" in {
      val t = newStacked()
      t.notOverriddenStacked must be("foobar")
      t.stacked must be("FOOBAR")
      mustStop(t)
    }

    "be able to support implementation only typed actors" in {
      val t = typedActorOf[Foo, Bar](Configuration())
      val f = t.futurePigdog(200)
      val f2 = t.futurePigdog(0)
      f2.isCompleted must be(false)
      f.isCompleted must be(false)
      f.get must equal(f2.get)
      mustStop(t)
    }

    "be able to support implementation only typed actors with complex interfaces" in {
      val t = typedActorOf[Stackable1 with Stackable2, StackedImpl]()
      t.stackable1 must be("foo")
      t.stackable2 must be("bar")
      mustStop(t)
    }

    "be able to use work-stealing dispatcher" in {
      val config = Configuration(
        Duration(6600, "ms"),
        Dispatchers.newBalancingDispatcher("pooled-dispatcher")
          .withNewThreadPoolWithLinkedBlockingQueueWithUnboundedCapacity
          .setCorePoolSize(60)
          .setMaxPoolSize(60)
          .build)

      val thais = for (i ← 1 to 60) yield newFooBar(config)
      val iterator = new CyclicIterator(thais)

      val results = for (i ← 1 to 120) yield (i, iterator.next.futurePigdog(200L, i))

      for ((i, r) ← results) r.get must be("Pigdog" + i)

      for (t ← thais) mustStop(t)
    }

    "be able to serialize and deserialize invocations" in {
      import java.io._
      val m = MethodCall(classOf[Foo].getDeclaredMethod("pigdog"), Array[AnyRef]())
      val baos = new ByteArrayOutputStream(8192 * 4)
      val out = new ObjectOutputStream(baos)

      out.writeObject(m)
      out.close()

      val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

      val mNew = in.readObject().asInstanceOf[MethodCall]

      mNew.method must be(m.method)
    }

    "be able to serialize and deserialize invocations' parameters" in {
      import java.io._
      val someFoo: Foo = new Bar
      val m = MethodCall(classOf[Foo].getDeclaredMethod("futureComposePigdogFrom", Array[Class[_]](classOf[Foo]): _*), Array[AnyRef](someFoo))
      val baos = new ByteArrayOutputStream(8192 * 4)
      val out = new ObjectOutputStream(baos)

      out.writeObject(m)
      out.close()

      val in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray))

      val mNew = in.readObject().asInstanceOf[MethodCall]

      mNew.method must be(m.method)
      mNew.parameters must have size 1
      mNew.parameters(0) must not be null
      mNew.parameters(0).getClass must be === classOf[Bar]
    }
  }
}
