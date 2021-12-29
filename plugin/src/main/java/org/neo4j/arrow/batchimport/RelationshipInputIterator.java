package org.neo4j.arrow.batchimport;

import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;

public class RelationshipInputIterator implements InputIterator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelationshipInputIterator.class);

    private boolean calledYet = false;

    @Override
    public InputChunk newChunk() {
        logger.info("newChunk()");
        return new InputChunk() {
            private int fed = 0;
            @Override
            public boolean next(InputEntityVisitor visitor) throws IOException {
                logger.info("InputChunk.next({})", visitor);

                fed++;
                if (fed <= 1) {
                    visitor.startId(0);
                    visitor.endId(1);
                    visitor.type("MY_REL");
                    visitor.endOfEntity();
                    return true;
                }
                return false;
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public boolean next(InputChunk chunk) throws IOException {
        logger.info("next({})", chunk);

        if (!calledYet) {
            calledYet = true;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {

    }
}
