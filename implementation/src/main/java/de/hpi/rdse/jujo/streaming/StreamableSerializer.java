package de.hpi.rdse.jujo.streaming;

import akka.util.ByteString;

import java.io.NotSerializableException;

public interface StreamableSerializer {

    ByteString serialize(Streamable message);

    StreamableDeserializationResult deserialize(ByteString message) throws NotSerializableException;

}
