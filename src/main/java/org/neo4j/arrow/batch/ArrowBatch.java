package org.neo4j.arrow.batch;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.reader.BaseReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * TBD
 */
public class ArrowBatch implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowBatch.class);

    private final String[] fieldNames;
    private final ValueVector[] vectors;
    private final int rowCount;

    protected ArrowBatch(String[] fieldNames, ValueVector[] vectors, int rowCount) {
        this.fieldNames = fieldNames;
        this.vectors = vectors;
        this.rowCount = rowCount;
    }

    /**
     * Build an {@link ArrowBatch} from the given {@link VectorSchemaRoot}, transferring buffers
     * to the given {@link BufferAllocator};
     *
     * @param root VectorSchemaRoot source for the ValueVectors
     * @param intoAllocator BufferAllocator to take ownership of the current Arrow buffers
     * @return a new ArrowBatch
     */
    public static ArrowBatch fromRoot(VectorSchemaRoot root, BufferAllocator intoAllocator) {
        final List<Field> fields = root.getSchema().getFields();
        final String[] fieldNames = fields.stream()
                .map(Field::getName).toArray(String[]::new);
        final ValueVector[] vectors = new ValueVector[fieldNames.length];

        IntStream.range(0, fieldNames.length)
                .forEach(idx -> {
                    try {
                        final FieldVector fv = root.getVector(idx);
                        final TransferPair pair = fv.getTransferPair(intoAllocator);
                        pair.transfer();
                        vectors[idx] = pair.getTo();
                    } catch (Exception e) {
                        logger.error("unable to build ArrowBatch", e);
                        throw new RuntimeException(e);
                    }
                });
        return new ArrowBatch(fieldNames, vectors, root.getRowCount());
    }

    @Override
    public void close() throws Exception {
        // The ArrowBatch doesn't own its allocator, so just free its vectors
        Arrays.stream(vectors).forEach(ValueVector::close);
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public ValueVector[] getVectors() {
        return vectors;
    }

    public int getRowCount() {
        return rowCount;
    }

    @Override
    public String toString() {
        return "ArrowBatch{" +
                "fieldNames=" + Arrays.toString(fieldNames) +
                ", rowCount=" + rowCount +
                '}';
    }
}
