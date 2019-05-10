package de.hpi.rdse.jujo.actors;

import akka.actor.Address;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class Slave extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "slave";

    public static Props props() {
        return Props.create(Slave.class);
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class RegisterAtShepherdMessage implements Serializable {
        private static final long serialVersionUID = -4399047760637406556L;
        private Address shepherdAddress;
        private int numberOfLocalWorkers;
    }

    @Data @Builder @NoArgsConstructor
    public static class AcknowledgementMessage implements Serializable {
        private static final long serialVersionUID = 3226726675135579564L;
    }

    private Slave() {

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegisterAtShepherdMessage.class, this::handle)
                .match(AcknowledgementMessage.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(RegisterAtShepherdMessage message)
    {

    }

    private void handle(AcknowledgementMessage message)
    {

    }
}
