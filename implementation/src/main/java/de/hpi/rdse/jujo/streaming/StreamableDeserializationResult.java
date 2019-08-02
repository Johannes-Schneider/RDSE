package de.hpi.rdse.jujo.streaming;

import akka.util.ByteString;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor @Getter
public class StreamableDeserializationResult {

    private final Streamable message;
    private final Class<? extends Streamable> messageType;
    private final ByteString remainingBytes;
    private final boolean isEmpty;

}
