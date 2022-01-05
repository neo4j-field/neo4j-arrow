package org.neo4j.arrow.batchimport;

import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RelationshipInputIterator implements InputIterator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelationshipInputIterator.class);


    @Override
    public InputChunk newChunk() {
        logger.info("newChunk()");
        return new InputChunk() {
            @Override
            public boolean next(InputEntityVisitor visitor) throws IOException {
                logger.info("InputChunk.next({})", visitor);
                visitor.startId(0);
                visitor.endId(1);
                visitor.type("MY_REL");
                visitor.endOfEntity();
                return false;
            }

            @Override
            public void close() throws IOException {
                logger.info("closeChunk()");
            }
        };
    }

    static AtomicBoolean calledYet = new AtomicBoolean(false);

    @Override
    public boolean next(InputChunk chunk) throws IOException {
        logger.info("next({})", chunk);

        if (!calledYet.compareAndExchange(false, true)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        logger.info("close()");

    }
}
