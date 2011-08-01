/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.camel

import java.lang.reflect.Method

import akka.actor._
import akka.camel.component.TypedActorComponent
import akka.event.EventHandler

/**
 * Concrete publish requestor that requests publication of typed consumer actor methods on
 * <code>ActorRegistered</code> events and unpublication of typed consumer actor methods on
 * <code>ActorUnregistered</code> events.
 *
 * @author Martin Krasser
 */
private[camel] class TypedConsumerPublishRequestor extends PublishRequestor {
  def receiveActorRegistryEvent = {
    case ActorRegistered(_, actor, typedActor)   ⇒ for (event ← ConsumerMethodRegistered.eventsFor(actor, typedActor)) deliverCurrentEvent(event)
    case ActorUnregistered(_, actor, typedActor) ⇒ for (event ← ConsumerMethodUnregistered.eventsFor(actor, typedActor)) deliverCurrentEvent(event)
  }
}

/**
 * Publishes a typed consumer actor method on <code>ConsumerMethodRegistered</code> events and
 * unpublishes a typed consumer actor method on <code>ConsumerMethodUnregistered</code> events.
 * Publications are tracked by sending an <code>activationTracker</code> an <code>EndpointActivated</code>
 * event, unpublications are tracked by sending an <code>EndpointActivated</code> event.
 *
 * @author Martin Krasser
 */
private[camel] class TypedConsumerPublisher(activationTracker: ActorRef) extends Actor {
  import TypedConsumerPublisher._

  def receive = {
    case mr: ConsumerMethodRegistered ⇒ {
      handleConsumerMethodRegistered(mr)
      activationTracker ! EndpointActivated
    }
    case mu: ConsumerMethodUnregistered ⇒ {
      handleConsumerMethodUnregistered(mu)
      activationTracker ! EndpointDeactivated
    }
    case _ ⇒ { /* ignore */ }
  }
}

/**
 * @author Martin Krasser
 */
private[camel] object TypedConsumerPublisher {
  /**
   * Creates a route to a typed actor method.
   */
  def handleConsumerMethodRegistered(event: ConsumerMethodRegistered) {
    CamelContextManager.mandatoryContext.addRoutes(new ConsumerMethodRouteBuilder(event))
    EventHandler notifyListeners EventHandler.Info(this, "published method %s of %s at endpoint %s" format (event.methodName, event.typedActor, event.endpointUri))
  }

  /**
   * Stops the route to the already un-registered typed consumer actor method.
   */
  def handleConsumerMethodUnregistered(event: ConsumerMethodUnregistered) {
    CamelContextManager.mandatoryContext.stopRoute(event.methodUuid)
    EventHandler notifyListeners EventHandler.Info(this, "unpublished method %s of %s from endpoint %s" format (event.methodName, event.typedActor, event.endpointUri))
  }
}

/**
 * Builder of a route to a typed consumer actor method.
 *
 * @author Martin Krasser
 */
private[camel] class ConsumerMethodRouteBuilder(event: ConsumerMethodRegistered) extends ConsumerRouteBuilder(event.endpointUri, event.methodUuid) {
  protected def routeDefinitionHandler: RouteDefinitionHandler = event.routeDefinitionHandler
  protected def targetUri = "%s:%s?method=%s" format (TypedActorComponent.InternalSchema, event.methodUuid, event.methodName)
}

/**
 * A typed consumer method (un)registration event.
 */
private[camel] trait ConsumerMethodEvent extends ConsumerEvent {
  val actorRef: ActorRef
  val typedActor: AnyRef
  val method: Method

  val uuid = actorRef.uuid.toString
  val methodName = method.getName
  val methodUuid = "%s_%s" format (uuid, methodName)

  lazy val routeDefinitionHandler = consumeAnnotation.routeDefinitionHandler.newInstance
  lazy val consumeAnnotation = method.getAnnotation(classOf[consume])
  lazy val endpointUri = consumeAnnotation.value
}

/**
 * Event indicating that a typed consumer actor has been registered at the actor registry. For
 * each <code>@consume</code> annotated typed actor method a separate event is created.
 */
private[camel] case class ConsumerMethodRegistered(actorRef: ActorRef, typedActor: AnyRef, method: Method) extends ConsumerMethodEvent

/**
 * Event indicating that a typed consumer actor has been unregistered from the actor registry. For
 * each <code>@consume</code> annotated typed actor method a separate event is created.
 */
private[camel] case class ConsumerMethodUnregistered(actorRef: ActorRef, typedActor: AnyRef, method: Method) extends ConsumerMethodEvent

/**
 * @author Martin Krasser
 */
private[camel] object ConsumerMethodRegistered {
  /**
   * Creates a list of ConsumerMethodRegistered event messages for a typed consumer actor or an empty
   * list if <code>actorRef</code> doesn't reference a typed consumer actor.
   */
  def eventsFor(actorRef: ActorRef, typedActor: Option[AnyRef]): List[ConsumerMethodRegistered] = {
    TypedConsumer.withTypedConsumer(actorRef, typedActor) { (tc, m) ⇒
      ConsumerMethodRegistered(actorRef, tc, m)
    }
  }
}

/**
 * @author Martin Krasser
 */
private[camel] object ConsumerMethodUnregistered {
  /**
   * Creates a list of ConsumerMethodUnregistered event messages for a typed consumer actor or an empty
   * list if <code>actorRef</code> doesn't reference a typed consumer actor.
   */
  def eventsFor(actorRef: ActorRef, typedActor: Option[AnyRef]): List[ConsumerMethodUnregistered] = {
    TypedConsumer.withTypedConsumer(actorRef, typedActor) { (tc, m) ⇒
      ConsumerMethodUnregistered(actorRef, tc, m)
    }
  }
}
