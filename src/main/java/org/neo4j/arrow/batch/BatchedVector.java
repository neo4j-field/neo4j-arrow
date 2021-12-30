package org.neo4j.arrow.batch;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.Text;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class BatchedVector implements Closeable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchedVector.class);

    private final List<ValueVector> vectors;
    private final int batchSize;
    private final String name;
    private final long rowCount;
    private int watermark = 0;

    BatchedVector(String name, List<ValueVector> vectors, int batchSize, long rowCount) {
        this.name = name;
        this.vectors = vectors;
        this.batchSize = batchSize;
        this.rowCount = rowCount;

        if (vectors == null || vectors.size() < 1)
            throw new RuntimeException("invalid vectors list");

        // XXX this is ugly...but we need to know where our search gets tough
        for (int i = 0; i < vectors.size(); i++) {
            final ValueVector vector = vectors.get(i);
            assert vector != null;
            if (vector.getValueCount() < batchSize) {
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
     *
     * @param index index of item to retrieve from the space
     * @return Object value if found, otherwise null
     */
    private Object translateIndex(long index) {
        assert (index < rowCount);

        // assumption is our batches only become "short" at the end
        int column = (int) Math.floorDiv(index, batchSize);
        int offset = (int) (index % batchSize);

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
            logger.error(String.format("failed to get index %d for %s (offset %d, column %d, batchSize %d)",
                    index, vectors.get(0).getName(), offset, column, batchSize), e);
            logger.trace(String.format("debug: %s",
                    vectors.stream()
                            .map(ValueVector::getValueCount)
                            .map(String::valueOf)
                            .collect(Collectors.joining(", "))));
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

    @Override
    public void close() {
        vectors.forEach(ValueVector::close);
    }
}
