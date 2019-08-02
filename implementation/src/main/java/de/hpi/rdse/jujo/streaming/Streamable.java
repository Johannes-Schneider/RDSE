package de.hpi.rdse.jujo.streaming;

import akka.actor.ActorRef;

import java.io.Serializable;

public interface Streamable extends Serializable {

    ActorRef getReceiver();

    ActorRef getSender();

}
