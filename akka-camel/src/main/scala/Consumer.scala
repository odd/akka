/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.camel

import akka.actor._

import java.net.InetSocketAddress

/**
 * Mixed in by Actor implementations that consume message from Camel endpoints.
 *
 * @author Martin Krasser
 */
trait Consumer { self: Actor =>
  /**
   * Returns the Camel endpoint URI to consume messages from.
   */
  def endpointUri: String

  /**
   * Determines whether two-way communications between an endpoint and this consumer actor
   * should be done in blocking or non-blocking mode (default is non-blocking). This method
   * doesn't have any effect on one-way communications (they'll never block). 
   */
  def blocking = false
}

/**
 * Java-friendly Consumer.
 *
 * @see UntypedConsumerActor
 * @see RemoteUntypedConsumerActor
 * @see UntypedConsumerTransactor
 *
 * @author Martin Krasser
 */
trait UntypedConsumer extends Consumer { self: UntypedActor =>
  final override def endpointUri = getEndpointUri
  final override def blocking = isBlocking

  /**
   * Returns the Camel endpoint URI to consume messages from.
   */
  def getEndpointUri(): String

  /**
   * Determines whether two-way communications between an endpoint and this consumer actor
   * should be done in blocking or non-blocking mode (default is non-blocking). This method
   * doesn't have any effect on one-way communications (they'll never block).
   */
  def isBlocking() = super.blocking
}

/**
 * Subclass this abstract class to create an MDB-style untyped consumer actor. This
 * class is meant to be used from Java.
 */
abstract class UntypedConsumerActor extends UntypedActor with UntypedConsumer

/**
 * Subclass this abstract class to create an MDB-style transacted untyped consumer
 * actor. This class is meant to be used from Java.
 */
abstract class UntypedConsumerTransactor extends UntypedTransactor with UntypedConsumer

/**
 * Subclass this abstract class to create an MDB-style remote untyped consumer
 * actor. This class is meant to be used from Java.
 */
abstract class RemoteUntypedConsumerActor(address: InetSocketAddress) extends RemoteUntypedActor(address) with UntypedConsumer {
  def this(host: String, port: Int) = this(new InetSocketAddress(host, port))
}

/**
 * @author Martin Krasser
 */
private[camel] object Consumer {
  /**
   * Applies a function <code>f</code> to <code>actorRef</code> if <code>actorRef</code>
   * references a consumer actor. A valid reference to a consumer actor is a local actor
   * reference with a target actor that implements the <code>Consumer</code> trait. The
   * target <code>Consumer</code> object is passed as argument to <code>f</code>. This
   * method returns <code>None</code> if <code>actorRef</code> is not a valid reference
   * to a consumer actor, <code>Some</code> consumer actor otherwise.
   */
  def forConsumer[T](actorRef: ActorRef)(f: Consumer => T): Option[T] = {
    if (!actorRef.actor.isInstanceOf[Consumer]) None
    else if (actorRef.remoteAddress.isDefined) None
    else Some(f(actorRef.actor.asInstanceOf[Consumer]))
  }
}
