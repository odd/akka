/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.amqp

import collection.JavaConversions

import akka.util.Logging

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, Envelope, DefaultConsumer}
import akka.amqp.AMQP.{NoActionDeclaration, ActiveDeclaration, PassiveDeclaration, ConsumerParameters}

private[amqp] class ConsumerActor(consumerParameters: ConsumerParameters)
        extends FaultTolerantChannelActor(
          consumerParameters.exchangeParameters, consumerParameters.channelParameters) {
  import consumerParameters._

  var listenerTag: Option[String] = None

  def specificMessageHandler = {
    case Acknowledge(deliveryTag) => acknowledgeDeliveryTag(deliveryTag, true)
    case Reject(deliveryTag) => rejectDeliveryTag(deliveryTag, true)
    case message: Message =>
      handleIllegalMessage("%s can't be used to send messages, ignoring message [%s]".format(this, message))
    case unknown =>
      handleIllegalMessage("Unknown message [%s] to %s".format(unknown, this))
  }

  protected def setupChannel(ch: Channel) = {

    val queueDeclare: com.rabbitmq.client.AMQP.Queue.DeclareOk = {
      queueName match {
        case Some(name) =>
          queueDeclaration match {
            case PassiveDeclaration =>
              log.debug("Passively declaring new queue [%s] for %s", name, toString)
              ch.queueDeclarePassive(name)
            case ActiveDeclaration(durable, autoDelete, exclusive) =>
              log.debug("Actively declaring new queue [%s] for %s", name, toString)
              val configurationArguments = exchangeParameters match {
                case Some(params) => params.configurationArguments
                case _ => Map.empty
              }
              ch.queueDeclare(name, durable, exclusive, autoDelete, JavaConversions.asMap(configurationArguments.toMap))
            case NoActionDeclaration => new com.rabbitmq.client.impl.AMQImpl.Queue.DeclareOk(name, 0, 0) // do nothing here
          }
        case None =>
          log.debug("Declaring new generated queue for %s", toString)
          ch.queueDeclare
      }
    }

    val exchangeName = exchangeParameters.flatMap(params => Some(params.exchangeName))
    log.debug("Binding new queue [%s] for %s", queueDeclare.getQueue, toString)
    ch.queueBind(queueDeclare.getQueue, exchangeName.getOrElse(""), routingKey)

    val tag = ch.basicConsume(queueDeclare.getQueue, false, new DefaultConsumer(ch) with Logging {
      override def handleDelivery(tag: String, envelope: Envelope, properties: BasicProperties, payload: Array[Byte]) {
        try {
          val deliveryTag = envelope.getDeliveryTag
          log.debug("Passing a message on to %s", toString)
          deliveryHandler ! Delivery(payload, envelope.getRoutingKey, envelope.getDeliveryTag, properties, someSelf)

          if (selfAcknowledging) {
            log.debug("Self acking...")
            acknowledgeDeliveryTag(deliveryTag, false)
          }
        } catch {
          case cause =>
            log.error(cause, "Delivery of message to %s failed", toString)
            self ! Failure(cause) // pass on and re-throw exception in consumer actor to trigger restart and connect
        }
      }
    })
    listenerTag = Some(tag)
    log.info("Intitialized %s", toString)
  }

  private def acknowledgeDeliveryTag(deliveryTag: Long, remoteAcknowledgement: Boolean) = {
    log.debug("Acking message with delivery tag [%s]", deliveryTag)
    channel.foreach {
      ch =>
        ch.basicAck(deliveryTag, false)
        if (remoteAcknowledgement) {
          deliveryHandler ! Acknowledged(deliveryTag)
        }
    }
  }

  private def rejectDeliveryTag(deliveryTag: Long, remoteAcknowledgement: Boolean) = {
    log.debug("Rejecting message with delivery tag [%s]", deliveryTag)
    // FIXME: when rabbitmq 1.9 arrives, basicReject should be available on the API and implemented instead of this
    log.warning("Consumer is rejecting delivery with tag [%s] - " +
            "for now this means we have to self terminate and kill the channel - see you in a second.")
    channel.foreach {
      ch =>
        if (remoteAcknowledgement) {
          deliveryHandler ! Rejected(deliveryTag)
        }
    }
    throw new RejectionException(deliveryTag)
  }

  private def handleIllegalMessage(errorMessage: String) = {
    log.error(errorMessage)
    throw new IllegalArgumentException(errorMessage)
  }

  override def preRestart(reason: Throwable) = {
    listenerTag = None
    super.preRestart(reason)
  }

  override def postStop = {
    listenerTag.foreach(tag => channel.foreach(_.basicCancel(tag)))
    self.shutdownLinkedActors
    super.postStop
  }

  override def toString =
    "AMQP.Consumer[id= " + self.id +
            ", exchangeParameters=" + exchangeParameters +
            ", queueDeclaration=" + queueDeclaration + "]"
}

