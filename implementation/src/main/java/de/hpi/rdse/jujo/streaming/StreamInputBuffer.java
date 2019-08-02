package de.hpi.rdse.jujo.streaming;

import java.util.concurrent.Callable;

public interface StreamInputBuffer {

    long size();

    boolean isEmpty();

    boolean isOpen();

    <T extends Streamable> void on(Callable<T> handler);
}
