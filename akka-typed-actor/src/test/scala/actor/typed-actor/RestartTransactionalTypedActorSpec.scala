/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.actor

import org.scalatest.Spec
import org.scalatest.Assertions
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import akka.config.Supervision._
import akka.actor._
import akka.config.{Config, TypedActorConfigurator}

@RunWith(classOf[JUnitRunner])
class RestartTransactionalTypedActorSpec extends
  Spec with
  ShouldMatchers with
  BeforeAndAfterAll {

  private val conf = new TypedActorConfigurator
  private var messageLog = ""

  def before {
    Config.config
    conf.configure(
      AllForOneStrategy(List(classOf[Exception]), 3, 5000),
      List(
        new SuperviseTypedActor(
          classOf[TransactionalTypedActor],
          Temporary,
          10000),
        new SuperviseTypedActor(
          classOf[TypedActorFailer],
          Temporary,
          10000)
      ).toArray).supervise
  }

  def after {
    conf.stop
    ActorRegistry.shutdownAll
  }

  describe("Restart supervised transactional Typed Actor ") {
/*
    it("map should rollback state for stateful server in case of failure") {
      before
      val stateful = conf.getInstance(classOf[TransactionalTypedActor])
      stateful.init
      stateful.setMapState("testShouldRollbackStateForStatefulServerInCaseOfFailure", "init")
      val failer = conf.getInstance(classOf[TypedActorFailer])
      try {
        stateful.failure("testShouldRollbackStateForStatefulServerInCaseOfFailure", "new state", failer)
        fail("should have thrown an exception")
      } catch { case e => {} }
      stateful.getMapState("testShouldRollbackStateForStatefulServerInCaseOfFailure") should equal("init")
      after
    }

    it("vector should rollback state for stateful server in case of failure") {
      before
      val stateful = conf.getInstance(classOf[TransactionalTypedActor])
      stateful.init
      stateful.setVectorState("init") // set init state
      val failer = conf.getInstance(classOf[TypedActorFailer])
      try {
        stateful.failure("testShouldRollbackStateForStatefulServerInCaseOfFailure", "new state", failer)
        fail("should have thrown an exception")
      } catch { case e => {} }
      stateful.getVectorState should equal("init")
      after
    }

    it("ref should rollback state for stateful server in case of failure") {
      val stateful = conf.getInstance(classOf[TransactionalTypedActor])
      stateful.init
      stateful.setRefState("init") // set init state
      val failer = conf.getInstance(classOf[TypedActorFailer])
      try {
        stateful.failure("testShouldRollbackStateForStatefulServerInCaseOfFailure", "new state", failer)
        fail("should have thrown an exception")
      } catch { case e => {} }
      stateful.getRefState should equal("init")
    }
*/  }
}
