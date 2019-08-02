package de.hpi.rdse.jujo.streaming;

import java.util.Iterator;

public interface StreamOutputBuffer extends Iterator<Streamable> {

    long size();

    boolean isEmpty();

    boolean isOpen();

    void close();

    void write(Streamable message);
}
