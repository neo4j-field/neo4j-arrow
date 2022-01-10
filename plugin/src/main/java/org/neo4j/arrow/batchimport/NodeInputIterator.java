package org.neo4j.arrow.batchimport;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.Text;
import org.neo4j.arrow.batch.ArrowBatch;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeInputIterator implements QueueInputIterator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NodeInputIterator.class);

    private final BlockingQueue<ArrowBatch> queue;
    private final AtomicBoolean queueOpen = new AtomicBoolean(true);
    private final String idField;
    private final String labelsField;

    public static QueueInputIterator fromQueue(BlockingQueue<ArrowBatch> queue, String idField, String labelsField) {
        return new NodeInputIterator(queue, idField, labelsField);
    }

    public NodeInputIterator(BlockingQueue<ArrowBatch> queue, String idField, String labelsField) {
        this.queue = queue;
        this.idField = idField;
        this.labelsField = labelsField;
        logger.info("created {}", this);
    }

    @Override
    public void closeQueue() {
        logger.info("closing queues");
        queueOpen.set(false);
    }

    /**
     * Needs to be given {@link ArrowBatch} instances for processing. Should
     * close each {@link ArrowBatch} as well. NOT THREAD SAFE.
     */
    private static class NodeChunk implements InputChunk {
        private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NodeChunk.class);

        private ArrowBatch batch = null;
        private int row = 0;
        private int nodeIdIndex = -1;
        private int labelsIndex = -1;

        public void offerBatch(ArrowBatch batch, int nodeIndex, int labelsIndex) {
            logger.info("setting batch {}, nodeIndex = {}, labelsIndex = {}", batch, nodeIndex, labelsIndex);

            if (this.batch != null) {
                // XXX should we be the ones closing this?
                AutoCloseables.closeNoChecked(this.batch);
            }

            this.batch = batch;
            this.nodeIdIndex = nodeIndex;
            this.labelsIndex = labelsIndex;

            row = 0;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            logger.trace("next() @ {}", row);

            // Process a single "row" from the batch until we figure out the API
            try {
                final ValueVector[] vectors = batch.getVectors();
                final String[] fieldNames = batch.getFieldNames();

                // XXX Assume our vectors are properly typed for now. (Yuck, casts!)
                for (int idx = 0; idx < vectors.length; idx++) {
                    if (idx == nodeIdIndex) {
                        final long nodeId = ((BigIntVector) vectors[idx]).get(row);
                        visitor.id(nodeId);
                    } else if (idx == labelsIndex) {
                        final String[] labels = ((ListVector) vectors[idx]).getObject(row)
                                .stream().map(Object::toString).toArray(String[]::new);
                        visitor.labels(labels);
                    } else {
                        final ValueVector vector = vectors[idx];

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
                        }  else {
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
        return new NodeChunk();
    }

    @Override
    public boolean next(InputChunk chunk) {
        assert (chunk instanceof NodeChunk);
        try {
            final NodeChunk nodeChunk = (NodeChunk) chunk;

            // XXX poll interval guess
            ArrowBatch batch = null;
            while (queueOpen.get() || !queue.isEmpty()) {
                batch = queue.poll(500, TimeUnit.MILLISECONDS);

                if (batch != null) {
                    logger.info("building NodeChunk from batch {}", batch);
                    // Assume only that field names are in same order as the vectors
                    final String[] names = batch.getFieldNames();
                    assert (names != null);

                    int nodeIdIndex = -1;
                    int labelsIndex = -1;

                    for (int i=0; i<names.length; i++) {
                        if (idField.equalsIgnoreCase(names[i])) {
                            nodeIdIndex = i;
                        } else if (labelsField.equalsIgnoreCase(names[i])) {
                            labelsIndex = i;
                        }
                        if (nodeIdIndex >= 0 && labelsIndex >= 0)
                            break;
                    }

                    // TODO validate id and label indices are not garbage
                    assert (nodeIdIndex >= 0 && labelsIndex >= 0);

                    nodeChunk.offerBatch(batch, nodeIdIndex, labelsIndex);
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
        // TODO should this even happen? What's the API here?
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
        return "NodeInputIterator{" +
                "queueOpen=" + queueOpen +
                ", idField='" + idField + '\'' +
                ", labelsField='" + labelsField + '\'' +
                '}';
    }
}
