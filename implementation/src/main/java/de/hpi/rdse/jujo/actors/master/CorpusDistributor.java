package de.hpi.rdse.jujo.actors.master;

import akka.NotUsed;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import akka.util.ByteString;
import com.esotericsoftware.minlog.Log;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.slave.CorpusReceiver;
import de.hpi.rdse.jujo.fileHandling.FilePartition;
import de.hpi.rdse.jujo.fileHandling.FilePartitionIterator;
import de.hpi.rdse.jujo.fileHandling.FilePartitioner;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class CorpusDistributor extends AbstractReapedActor {

    public static Props props(int expectedNumberOfSlaves, Path corpusFilePath) {
        return Props.create(CorpusDistributor.class,
                            () -> new CorpusDistributor(expectedNumberOfSlaves, corpusFilePath));
    }

    @NoArgsConstructor
    public static class AcknowledgeCorpusPartition implements Serializable {
        private static final long serialVersionUID = 3477713584834122249L;
    }

    private final Path corpusFilePath;
    private final int expectedNumberOfSlaves;
    private final Set<RootActorPath> acknowledgedCorpusReceivers = new HashSet<>();
    private final Map<RootActorPath, Source<ByteString, NotUsed>> corpusSources = new HashMap<>();
    private final FilePartitioner filePartitioner;
    private final Materializer materializer;

    private CorpusDistributor(int expectedNumberOfSlaves, Path corpusFilePath) {
        this.expectedNumberOfSlaves = expectedNumberOfSlaves;
        this.corpusFilePath = corpusFilePath;
        this.materializer = ActorMaterializer.create(this.context().system());
        this.filePartitioner = new FilePartitioner(corpusFilePath, this.expectedNumberOfSlaves);
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(Shepherd.SlaveNodeRegistrationMessage.class, this::handle)
                   .match(AcknowledgeCorpusPartition.class, this::handle)
                   .build();
    }

    private void handle(Shepherd.SlaveNodeRegistrationMessage message) {
        if (!this.corpusSources.containsKey(message.getSlave().path().root())) {
            this.corpusSources.put(message.getSlave().path().root(), createCorpusSource());
        }
        this.log().info(String.format("Starting distributing corpus partition to %s", message.getSlave().path()));
        Source<ByteString, NotUsed> source = corpusSources.get(message.getSlave().path().root());
        CompletionStage<SourceRef<ByteString>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.materializer);

        Patterns.pipe(sourceRef.thenApply(CorpusReceiver.ProcessCorpusPartition::new), this.context().dispatcher()).to(
                message.getSlave());
    }

    private Source<ByteString, NotUsed> createCorpusSource() {
        FilePartition filePartition = this.filePartitioner.getNextPartition();
        try {
            FilePartitionIterator filePartitionIterator = new FilePartitionIterator(filePartition,
                                                                                    this.corpusFilePath.toFile());
            return Source.<ByteString>fromIterator(() -> filePartitionIterator);
        } catch (IOException e) {
            Log.error("Unable to create iterator for FilePartition.", e);
            return Source.<ByteString>empty();
        }
    }

    private void handle(AcknowledgeCorpusPartition message) {
        this.acknowledgedCorpusReceivers.add(this.sender().path().root());

        if (this.acknowledgedCorpusReceivers.size() == this.expectedNumberOfSlaves) {
            this.purposeHasBeenFulfilled();
        }
    }
}
