package org.neo4j.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Container of Arrow vectors and metadata, schema, etc.
 * <p>
 *     This is a mess right now :-(
 * </p>
 */
public class ArrowBatch implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowBatch.class);

    final Schema schema;
    final BufferAllocator allocator;

    final List<List<ValueVector>> vectorSpace = new ArrayList<>();
    final String[] fieldNames;

    long rowCount = 0;
    int batchSize = -1;

    public ArrowBatch(Schema schema, BufferAllocator parentAllocator, String name) {
        this.schema = schema;
        this.allocator = parentAllocator.newChildAllocator("arrow-batch-" + name, 0, Long.MAX_VALUE);

        final List<Field> fields = schema.getFields();
        fieldNames = new String[fields.size()];

        IntStream.range(0, fields.size())
                .forEach(idx -> {
                    fieldNames[idx] = fields.get(idx).getName();
                    vectorSpace.add(new ArrayList<>());
                });
        rowCount = 0;
    }

    public void appendRoot(VectorSchemaRoot root) {
        // TODO: validate schema option?

        if (batchSize < 0) {
            batchSize = root.getRowCount();
        }

        final List<FieldVector> incoming = root.getFieldVectors();
        final int cols = incoming.size();
        logger.debug("appending root with {} rows, {} vectors", root.getRowCount(), cols);
        IntStream.range(0, cols)
                .forEach(idx -> {
                    final FieldVector fv = incoming.get(idx);
                    final int valueCount = fv.getValueCount();

                    final TransferPair pair = fv.getTransferPair(allocator);
                    pair.transfer();
                    final ValueVector to = pair.getTo();
                    to.setValueCount(valueCount);
                    vectorSpace.get(idx).add(to);
                    fv.close();
                });

        rowCount += root.getRowCount();
        logger.debug("new rowcount {}", rowCount);
    }

    public static class BatchedVector {
        private final List<ValueVector> vectors;
        private final int batchSize;
        private final String name;

        private BatchedVector(String name, List<ValueVector> vectors, int batchSize) {
            this.name = name;
            this.vectors = vectors;
            this.batchSize = batchSize;
        }

        public String getName() {
            return name;
        }

        public Class<?> getBaseType() {
            return vectors.get(0).getClass();
        }

        /**
         * Find an item from the vector space, accounting for the fact the tail end might have batch sizes less than
         * the original batch size.
         * @param index index of item to retrieve from the space
         * @return Object value if found, otherwise null
         */
        private Object translateIndex(long index) {
            // assumption is our batches only become "short" at the end
            int column = (int) (index / batchSize);
            int offset = (int) (index % batchSize);

            try {
                ValueVector vector = vectors.get(column);
                if (vector.getValueCount() > offset) {
                    // easy peasy
                    return vector.getObject(offset);
                }

                while (vector.getValueCount() <= offset) {
                    // we need to advance out pointers and offsets :-(
                    column++;
                    offset = offset - vector.getValueCount();
                    vector = vectors.get(column);
                }
                return vector.getObject(offset);
            } catch (Exception e) {
                logger.error(String.format("failed to get index %d (offset %d, column %d, batchSize %d)", index, offset, column, batchSize), e);
                return null;
            }
        }

        public long getNodeId(long index) {
            final Long nodeId = (Long) translateIndex(index);
            if (nodeId == null) {
                throw new RuntimeException(String.format("cant get nodeid for index %d", index));
            }
            if (nodeId < 0) {
                throw new RuntimeException("nodeId < 0?!?!");
            }
            return nodeId;
        }

        public List<?> getLabels(long index) {
            final List<?> list = (List<?>) translateIndex(index);
            if (list == null) {
                logger.warn("failed to find list at index {}, index", index);
                return List.of();
            }
            return list;
        }

        public String getType(long index) {
            // XXX Assumption for now is we're dealing with a VarCharVector
            final Text type = (Text) translateIndex(index);
            if (type == null) {
                logger.warn("failed to find type string at index {}, index", index);
                return "";
            }
            return type.toString();
        }

        public Object getObject(long index) {
            final Object o = translateIndex(index);
            if (o == null) {
                logger.warn("failed to find list at index {}, index", index);
                return null;
            }
            return o;
        }

        public List<?> getList(long index) {
            final List<?> list = (List<?>) translateIndex(index);
            if (list == null) {
                logger.warn("failed to find list at index {}, index", index);
                return List.of();
            }
            return list;
        }

        public Optional<Class<?>> getDataClass() {
            final ValueVector v = vectors.get(0);
            if (v instanceof ListVector) {
                return Optional.of(((ListVector) v).getDataVector().getClass());
            } else if (v instanceof FixedSizeListVector) {
                final FixedSizeListVector fv = (FixedSizeListVector) v;
                return Optional.of(fv.getDataVector().getClass());
            }
            return Optional.empty();
        }
    }

    public BatchedVector getVector(int index) {
        if (index < 0 || index >= fieldNames.length)
            throw new RuntimeException("index out of range");

        return new BatchedVector(fieldNames[index], vectorSpace.get(index), batchSize);
    }

    public BatchedVector getVector(String name) {
        logger.info("...finding vector for name {}", name);
        int index = 0;
        for ( ; index<fieldNames.length; index++) {
            if (fieldNames[index].equals(name))
                break;
        }
        if (index == fieldNames.length)
            throw new RuntimeException(String.format("name %s not found in arrow batch", name));

        return new BatchedVector(name, vectorSpace.get(index), batchSize);
    }

    public List<BatchedVector> getFieldVectors() {
        return IntStream.range(0, fieldNames.length)
                .mapToObj(this::getVector)
                .collect(Collectors.toList());
    }

    public int getRowCount() {
        return (int) rowCount; // XXX  TODO
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void close() {
        try {
            for (List<ValueVector> list : vectorSpace) {
                AutoCloseables.close(list);
            }
            AutoCloseables.close(allocator);
        } catch (Exception e) {
            e.printStackTrace(); // TODO logger
        }
    }
}
