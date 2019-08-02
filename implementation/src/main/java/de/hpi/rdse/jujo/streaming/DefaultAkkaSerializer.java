package de.hpi.rdse.jujo.streaming;

import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import akka.util.ByteString;
import org.reflections.Reflections;

import java.io.NotSerializableException;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DefaultAkkaSerializer implements StreamableSerializer {

    private static final int SERIALIZATION_HEADER_LENGTH = Long.BYTES + Integer.BYTES;

    private final Serialization serializationExtension;
    private final Reflections reflections;
    private final Map<Long, Class<? extends Streamable>> messageClassesBySerialVersionUID = new HashMap<>();

    public DefaultAkkaSerializer(ActorSystem actorSystem) {
        this.serializationExtension = SerializationExtension.get(actorSystem);
        this.reflections = new Reflections("de.hpi.rdse.jujo");
        this.fillMessageTypesBySerialVersionUID();
    }

    private void fillMessageTypesBySerialVersionUID() {
        for (Class<? extends Streamable> messageClass : this.reflections.getSubTypesOf(Streamable.class)) {
            long uid = ObjectStreamClass.lookup(messageClass).getSerialVersionUID();
            this.messageClassesBySerialVersionUID.put(uid, messageClass);
        }
    }

    @Override
    public ByteString serialize(Streamable message) {
        Serializer serializer = this.serializationExtension.findSerializerFor(message);
        long uid = ObjectStreamClass.lookup(message.getClass()).getSerialVersionUID();
        byte[] serializedMessage = serializer.toBinary(message);

        ByteBuffer buffer = ByteBuffer.allocate(SERIALIZATION_HEADER_LENGTH + serializedMessage.length);
        buffer.putLong(uid);
        buffer.putInt(serializedMessage.length);
        buffer.put(serializedMessage);

        return ByteString.fromByteBuffer(buffer);
    }

    @Override
    public StreamableDeserializationResult deserialize(ByteString message) throws NotSerializableException {
        int bufferSize = message.length();

        if (bufferSize < DefaultAkkaSerializer.SERIALIZATION_HEADER_LENGTH) {
            return this.emptyResult(message);
        }

        ByteBuffer buffer = message.toByteBuffer();

        long uid = buffer.getLong();
        int messageLength = buffer.getInt();

        if (bufferSize < DefaultAkkaSerializer.SERIALIZATION_HEADER_LENGTH + messageLength) {
            return this.emptyResult(message);
        }

        return this.deserialize(buffer, bufferSize, uid, messageLength);
    }

    private StreamableDeserializationResult emptyResult(ByteString message) {
        return new StreamableDeserializationResult(null, null, message, true);
    }

    private StreamableDeserializationResult deserialize(ByteBuffer buffer, int bufferSize, long serialVersionUID, int messageLength) throws NotSerializableException {
        Class<? extends Streamable> messageType = this.messageClassesBySerialVersionUID.get(serialVersionUID);

        byte[] serializedMessage = new byte[messageLength];
        buffer.get(serializedMessage);

        byte[] remainingBytes = new byte[bufferSize - buffer.position()];
        buffer.get(remainingBytes);

        Serializer serializer = this.serializationExtension.serializerFor(messageType);
        Object message = serializer.fromBinary(serializedMessage);

        return new StreamableDeserializationResult((Streamable) message, messageType,
                                                   ByteString.fromArray(remainingBytes), false);
    }
}
