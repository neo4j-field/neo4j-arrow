package org.neo4j.arrow.batch;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.neo4j.arrow.Config;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Container of Arrow vectors and metadata, schema, etc. !!! NOT THREAD SAFE.!!!
 * <p>
 *     This is a mess right now :-(
 * </p>
 */
public class ArrowBatches implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowBatches.class);

    final Schema schema;
    private BufferAllocator allocator;
    final List<List<ValueVector>> vectorSpace = new ArrayList<>();
    final String[] fieldNames;

    long rowCount = 0;
    int maxBatchSize = -1;

    public ArrowBatches(Schema schema, BufferAllocator parentAllocator, String name) {
        this.schema = schema;
        this.allocator = parentAllocator.newChildAllocator(name, 0L, Config.maxStreamMemory);
        final List<Field> fields = schema.getFields();
        fieldNames = new String[fields.size()];

        IntStream.range(0, fields.size())
                .forEach(idx -> {
                    final String fieldName = fields.get(idx).getName();
                    fieldNames[idx] = fieldName;
                    vectorSpace.add(new ArrayList<>());
                    logger.info("added {} to vectorspace", fieldName);
                });
    }

    public void appendBatch(ArrowBatch batch) {
        assert batch != null;
        final ValueVector[] vectors = batch.getVectors();
        final int rows = batch.getRowCount();

        if (rows < 1) {
            throw new RuntimeException("empty batch?");
        }

        maxBatchSize = Math.max(maxBatchSize, rows);

        assert vectors.length == fieldNames.length;
        IntStream.range(0, vectors.length)
                .forEach(idx -> {
                    final List<ValueVector> vectorList = vectorSpace.get(idx);
                    final TransferPair pair = vectors[idx].getTransferPair(allocator);
                    pair.transfer();
                    vectorList.add(pair.getTo());
                });

        rowCount += rows;

        AutoCloseables.closeNoChecked(batch);
    }

    public long estimateSize() {
        return vectorSpace.stream()
                .flatMap(Collection::stream)
                .mapToLong(ValueVector::getBufferSize)
                .sum();
    }

    public long actualSize() {
        return vectorSpace.stream()
                .flatMap(Collection::stream)
                .map(vec -> vec.getBuffers(false))
                .flatMap(Arrays::stream)
                .mapToLong(ArrowBuf::capacity)
                .sum();
    }

    public BatchedVector getVector(int index) {
        if (index < 0 || index >= fieldNames.length)
            throw new RuntimeException("index out of range");

        final List<ValueVector> vectors = vectorSpace.get(index);
        if (vectors == null) {
            System.out.println("index " + index + " returned nul list?!");
            throw new ArrayIndexOutOfBoundsException("invalid vectorspace index");
        }
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
        return (int) rowCount; // XXX cast
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void close() {
        allocator.assertOpen();
        vectorSpace.forEach(list -> list.forEach(ValueVector::close));
        allocator.close();
    }
}
