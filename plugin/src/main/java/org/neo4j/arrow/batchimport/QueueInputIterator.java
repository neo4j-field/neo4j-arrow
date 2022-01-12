package org.neo4j.arrow.batchimport;

import org.neo4j.internal.batchimport.InputIterator;

public interface QueueInputIterator extends InputIterator {
    void closeQueue();
    boolean isOpen();
}
