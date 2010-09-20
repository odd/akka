package sample.camel;

import se.scalablesolutions.akka.actor.TypedActor;

/**
 * @author Martin Krasser
 */
public class RemoteTypedConsumer1Impl extends TypedActor implements RemoteTypedConsumer1 {

    public String foo(String body, String header) {
        return String.format("remote1: body=%s header=%s", body, header);
    }
}
