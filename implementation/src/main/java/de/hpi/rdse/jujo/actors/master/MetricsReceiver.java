package de.hpi.rdse.jujo.actors.master;

import akka.actor.Props;
import akka.actor.Terminated;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.metrics.ClusterMetricsChanged;
import akka.cluster.metrics.ClusterMetricsExtension;
import akka.cluster.metrics.NodeMetrics;
import akka.cluster.metrics.StandardMetrics;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;

public class MetricsReceiver extends AbstractReapedActor {

    public static Props props() {
        return Props.create(MetricsReceiver.class, MetricsReceiver::new);
    }

    private final ClusterMetricsExtension clusterMetricsExtension = ClusterMetricsExtension.get(getContext().system());

    @Override
    public void preStart() {
        clusterMetricsExtension.subscribe(getSelf());
    }

    @Override
    public void postStop() {
        clusterMetricsExtension.unsubscribe(getSelf());
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(ClusterMetricsChanged.class, this::handle)
                   .match(CurrentClusterState.class, message -> {/*Ignore*/})
                   .match(Shepherd.SlaveNodeRegistrationMessage.class, this::handle)
                   .match(Terminated.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(ClusterMetricsChanged message) {
        for (NodeMetrics nodeMetrics : message.getNodeMetrics()) {
            logHeap(nodeMetrics);
            logCpu(nodeMetrics);
        }
    }

    private void handle(Shepherd.SlaveNodeRegistrationMessage message) {
        this.context().watch(message.getSlave());
        this.clusterMetricsExtension.subscribe(message.getSlave());
    }

    private void logHeap(NodeMetrics nodeMetrics) {
        StandardMetrics.HeapMemory heap = StandardMetrics.extractHeapMemory(nodeMetrics);
        if (heap != null) {
            this.log().info("[{}] Used heap: {} MB", nodeMetrics.address(), ((double) heap.used()) / 1024 / 1024);
        }
    }

    private void logCpu(NodeMetrics nodeMetrics) {
        StandardMetrics.Cpu cpu = StandardMetrics.extractCpu(nodeMetrics);
        if (cpu != null && cpu.systemLoadAverage().isDefined()) {
            this.log().info("[{}] Load: {} ({} processors)", nodeMetrics.address(), cpu.systemLoadAverage().get(),
                    cpu.processors());
        }
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.actor());
        this.clusterMetricsExtension.unsubscribe(message.actor());
    }

}
