package org.neo4j.arrow.batchimport;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.values.storable.Values;

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
        logger.trace("closing Relationship queue");
        queueOpen.set(false);
    }

    @Override
    public boolean isOpen() {
        return queueOpen.get();
    }

    private static class RelsChunk implements InputChunk {
        private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelsChunk.class);

        private ArrowBatch batch = null;
        private int row = 0;
        private int sourceIndex = -1;
        private int targetIndex = -1;
        private int typeIndex = -1;

        public void offerBatch(ArrowBatch batch, int sourceIndex, int targetIndex, int typeIndex) {
            logger.debug("setting batch {}, sourceIndex = {}, targetIndex = {}, typeIndex = {}",
                    batch, sourceIndex, targetIndex, typeIndex);

            if (this.batch != null) {
                // XXX should we be the ones closing this?
                AutoCloseables.closeNoChecked(this.batch);
            }

            this.batch = batch;
            this.sourceIndex = sourceIndex;
            this.targetIndex = targetIndex;
            this.typeIndex = typeIndex;

            row = 0;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) {
            logger.trace("next() @ {}", row);

            // Process a single "row" from the batch until we figure out the API
            try {
                final ValueVector[] vectors = batch.getVectors();
                final String[] fieldNames = batch.getFieldNames();

                for (int idx = 0; idx < vectors.length; idx++) {
                    final ValueVector vector = vectors[idx];

                    if (idx == sourceIndex) {
                        final long sourceId = ((BigIntVector) vector).get(row);
                        visitor.startId(sourceId);
                    } else if (idx == targetIndex) {
                        final long targetId = ((BigIntVector) vector).get(row);
                        visitor.endId(targetId);
                    } else if (idx == typeIndex) {
                        final String type = new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
                        visitor.type(type);
                    } else if (!vector.isNull(row)) {
                        // Skip null.
                        // TODO: the type detection should be cached. Should this be pulled up into ArrowBatch?
                        if (vector instanceof VarCharVector) {
                            final byte[] bytes = ((VarCharVector) vector).get(row);
                            visitor.property(fieldNames[idx], new String(bytes, StandardCharsets.UTF_8));
                        } else if (vector instanceof BaseListVector) {
                            final Object value = vector.getObject(row);
                            if (value instanceof JsonStringArrayList<?>) {
                                // XXX Assume homogeneity as that's what the DB supports
                                final int len = ((JsonStringArrayList<?>) value).size();
                                if (len > 0) {
                                    final Object head = ((JsonStringArrayList<?>) value).get(0);
                                    Object[] values = null;
                                    if (head instanceof Integer) {
                                        values = new Integer[len];
                                    } else if (head instanceof Long) {
                                        values = new Long[len];
                                    } else if (head instanceof Double) {
                                        values = new Double[len];
                                    } else if (head instanceof Float) {
                                        values = new Float[len];
                                    } else if (head instanceof String) {
                                        values = new String[len];
                                    } else {
                                        logger.warn("unhandled JsonStringArrayList type: {}", head.getClass());
                                    }
                                    if (values != null) {
                                        ((JsonStringArrayList<?>) value).toArray(values);
                                        visitor.property(fieldNames[idx], values);
                                    }
                                } else {
                                    visitor.property(fieldNames[idx], Values.EMPTY_INT_ARRAY);
                                }
                            } else {
                                // XXX Hopefully this is a usable list ;)
                                visitor.property(fieldNames[idx], value);
                            }
                        } else if (vector instanceof Float4Vector) {
                            final float value = ((Float4Vector) vector).get(row);
                            if (Float.isFinite(value))
                                visitor.property(fieldNames[idx], value);
                        } else if (vector instanceof Float8Vector) {
                            final double value = ((Float8Vector) vector).get(row);
                            if (Double.isFinite(value))
                                visitor.property(fieldNames[idx], value);
                        } else {
                            // And the rest...
                            final Object value = vector.getObject(row);
                            try {
                                visitor.property(fieldNames[idx], value);
                            } catch (IllegalArgumentException iae) {
                                logger.warn(String.format("failed to set property from field '%s'", fieldNames[idx]), iae);
                            }
                        }
                    }
                }
                visitor.endOfEntity();
            } catch (ClassCastException cce) {
                logger.error("class cast issue!", cce);
                throw new RuntimeException("class cast issue!");
            } catch (Exception e) {
                logger.error("oh crap", e);
            }

            row++;
            return (row < batch.getRowCount());
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
            while (queueOpen.get() || !queue.isEmpty()) {
                final ArrowBatch batch = queue.poll(500, TimeUnit.MILLISECONDS);

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
        logger.trace("done producing RelsChunks");
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
