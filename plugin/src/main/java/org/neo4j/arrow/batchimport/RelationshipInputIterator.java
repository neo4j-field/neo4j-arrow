package org.neo4j.arrow.batchimport;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RelationshipInputIterator implements QueueInputIterator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelationshipInputIterator.class);

    private final BlockingQueue<ArrowBatch> queue;
    private final AtomicBoolean queueOpen = new AtomicBoolean(true);
    private final String sourceField;
    private final String targetField;
    private final String typeField;

    public static QueueInputIterator fromQueue(BlockingQueue<ArrowBatch> queue, String sourceField, String targetField, String typeField) {
        return new RelationshipInputIterator(queue, sourceField, targetField, typeField);
    }

    public RelationshipInputIterator(BlockingQueue<ArrowBatch> queue, String sourceField, String targetField, String typeField) {
        this.queue = queue;
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.typeField = typeField;
    }

    @Override
    public void closeQueue() {
        logger.info("closing queues");
        queueOpen.set(false);
    }

    private static class RelsChunk implements InputChunk {
        private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelsChunk.class);

        private ArrowBatch batch = null;
        private int index = 0;
        private int sourceIndex = -1;
        private int targetIndex = -1;
        private int typeIndex = -1;

        public void offerBatch(ArrowBatch batch, int sourceIndex, int targetIndex, int typeIndex) {
            logger.info("setting batch {}, sourceIndex = {}, targetIndex = {}, typeIndex = {}",
                    batch, sourceIndex, targetIndex, typeIndex);

            if (this.batch != null) {
                // XXX should we be the ones closing this?
                AutoCloseables.closeNoChecked(this.batch);
            }

            this.batch = batch;
            this.sourceIndex = sourceIndex;
            this.targetIndex = targetIndex;
            this.typeIndex = typeIndex;

            index = 0;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            logger.trace("next() @ {}", index);

            // Process a single "row" from the batch until we figure out the API
            try {
                final ValueVector[] vectors = batch.getVectors();

                // XXX Assume our vectors are properly typed for now. (Casts!)
                final long sourceId = ((BigIntVector) vectors[sourceIndex]).get(index);
                final long targetId = ((BigIntVector) vectors[targetIndex]).get(index);
                final String type = new String(((VarCharVector) vectors[typeIndex]).get(index), StandardCharsets.UTF_8);

                visitor.startId(sourceId);
                visitor.endId(targetId);
                visitor.type(type);
                visitor.endOfEntity();
                // XXX skip properties for now
            } catch (Exception e) {
                logger.error("oh crap", e);
            }

            index++;
            return (index < batch.getRowCount());
        }

        @Override
        public void close() throws IOException {
            logger.trace("close()");
            try {
                if (batch != null)
                    batch.close();
            } catch (Exception e) {
                throw new IOException("error closing ArrowBatch", e);
            }
        }
    }

    @Override
    public InputChunk newChunk() {
        logger.trace("new chunk");
        return new RelsChunk();
    }

    @Override
    public boolean next(InputChunk chunk) {
        assert (chunk instanceof RelsChunk);

        try {
            final RelsChunk relsChunk = (RelsChunk) chunk;

            // XXX poll interval guess
            ArrowBatch batch = null;
            while (queueOpen.get() || !queue.isEmpty()) {
                batch = queue.poll(500, TimeUnit.MILLISECONDS);

                if (batch != null) {
                    logger.trace("building RelsChunk from batch {}", batch);
                    // Assume only that field names are in same order as the vectors
                    final String[] names = batch.getFieldNames();
                    assert (names != null);

                    int sourceIndex = -1;
                    int targetIndex = -1;
                    int typeIndex = -1;

                    for (int i=0; i<names.length; i++) {
                        if (sourceField.equalsIgnoreCase(names[i])) {
                            sourceIndex = i;
                        } else if (targetField.equalsIgnoreCase(names[i])) {
                            targetIndex = i;
                        } else if (typeField.equalsIgnoreCase(names[i])) {
                            typeIndex = i;
                        }
                        if (sourceIndex >= 0 && targetIndex >= 0 && typeIndex >= 0)
                            break;
                    }

                    // TODO validate indices are not garbage
                    assert (sourceIndex >= 0 && targetIndex >= 0 && typeIndex >= 0);

                    relsChunk.offerBatch(batch, sourceIndex, targetIndex, typeIndex);
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("oh crap", e);
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        logger.trace("close()");
        try {
            for (ArrowBatch batch : queue) {
                batch.close();
            }
        } catch (Exception e) {
            logger.error("error closing remaining ArrowBatches", e);
        }
    }

    @Override
    public String toString() {
        return "RelationshipInputIterator{" +
                "queueOpen=" + queueOpen +
                ", sourceField='" + sourceField + '\'' +
                ", targetField='" + targetField + '\'' +
                ", typeField='" + typeField + '\'' +
                '}';
    }
}
