package org.neo4j.arrow.batchimport;

import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;

public class RelationshipInputIterator implements InputIterator {
    @Override
    public InputChunk newChunk() {
        return new InputChunk() {
            @Override
            public boolean next(InputEntityVisitor visitor) throws IOException {
                return false;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public boolean next(InputChunk chunk) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
