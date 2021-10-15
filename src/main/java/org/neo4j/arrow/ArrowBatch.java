package org.neo4j.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
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
    int maxBatchSize = -1;

    public ArrowBatch(Schema schema, BufferAllocator parentAllocator, String name) {
        this.schema = schema;
        this.allocator = parentAllocator.newChildAllocator("arrow-batch-" + name, 0, Long.MAX_VALUE);

        final List<Field> fields = schema.getFields();
        fieldNames = new String[fields.size()];

        IntStream.range(0, fields.size())
                .forEach(idx -> {
                    final String fieldName = fields.get(idx).getName();
                    fieldNames[idx] = fieldName;
                    vectorSpace.add(new ArrayList<>());
                    logger.info("added {} to vectorspace", fieldName);
                });
        rowCount = 0;
    }

    public void appendRoot(VectorSchemaRoot root) {
        // TODO: validate schema option?
        if (maxBatchSize > 0 && root.getRowCount() > maxBatchSize) {
            logger.error("maxBatchSize: {}, root row count: {}", maxBatchSize, root.getRowCount());
            throw new RuntimeException("BOOP BOOP BOOP!");
        }
        maxBatchSize = Math.max(maxBatchSize, root.getRowCount());


        final List<FieldVector> incoming = root.getFieldVectors();
        final int cols = incoming.size();
        logger.trace("appending root with {} rows, {} vectors", root.getRowCount(), cols);
        IntStream.range(0, cols)
                .forEach(idx -> {
                    final FieldVector fv = incoming.get(idx);
                    logger.trace("fv: {}", fv);
                    final int valueCount = fv.getValueCount();

                    final TransferPair pair = fv.getTransferPair(allocator);
                    pair.transfer();
                    final ValueVector to = pair.getTo();
                    to.setValueCount(valueCount);
                    vectorSpace.get(idx).add(to);
                    fv.close();
                });

        rowCount += root.getRowCount();
        logger.trace("new rowcount {}", rowCount);
    }

    public static class BatchedVector {
        private final List<ValueVector> vectors;
        private final int batchSize;
        private final String name;
        private final long rowCount;
        private int watermark = 0;

        private BatchedVector(String name, List<ValueVector> vectors, int batchSize, long rowCount) {
            this.name = name;
            this.vectors = vectors;
            this.batchSize = batchSize;
            this.rowCount = rowCount;

            // XXX this is ugly...but we need to know where our search gets tough
            for (int i=0; i<vectors.size(); i++) {
                if (vectors.get(i).getValueCount() < batchSize) {
                    watermark = i;
                    break;
                }
            }
        }

        public List<ValueVector> getVectors() {
            return this.vectors;
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
            assert (index < rowCount);

            // assumption is our batches only become "short" at the end
            int column = (int) Math.floorDiv(index, batchSize);
            int offset = (int) (index % batchSize);
            //logger.info("looking up index {} (col = {}, offset = {})", index, column, offset);

            try {
                if (column < watermark) {
                    // trivial case
                    ValueVector vector = vectors.get(column);
                    return vector.getObject(offset);
                }

                // harder, we need to search varying size columns. start at our watermark.
                int pos = watermark * batchSize;
                column = watermark;
                ValueVector vector = vectors.get(column);
                logger.trace("starting search from pos {} to find index {} (watermark: {})", pos, index, watermark);
                while ((index - pos) >= vector.getValueCount()) {
                    column++;
                    pos += vector.getValueCount();
                    vector = vectors.get(column); // XXX eventually will barf...need better handling here
                }
                return vector.getObject((int) (index - pos));
            } catch (Exception e) {
                logger.error(String.format("failed to get index %d (offset %d, column %d, batchSize %d)", index, offset, column, batchSize), e);
                return null;
            }
        }

        public long getNodeId(long index) {
            final Long nodeId = (Long) translateIndex(index);
            if (nodeId == null) {
                throw new RuntimeException(String.format("cant get nodeId for index %d", index));
            }
            if (nodeId < 0) {
                throw new RuntimeException("nodeId < 0?!?!");
            }
            return nodeId;
        }

        public List<String> getLabels(long index) {
            final List<?> list = (List<?>) translateIndex(index);
            if (list == null) {
                logger.warn("failed to find list at index {}, index", index);
                return List.of();
            }
            return list.stream().map(Object::toString).collect(Collectors.toList());
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

        return new BatchedVector(fieldNames[index], vectorSpace.get(index), maxBatchSize, rowCount);
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

        return new BatchedVector(name, vectorSpace.get(index), maxBatchSize, rowCount);
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
