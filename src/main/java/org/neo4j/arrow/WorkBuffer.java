package org.neo4j.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Encapsulates the state of ValueVectors. Each WorkBuffer is fully isolated from others and contains its own
 * memory allocator. This class is explicitly NOT thread safe by design.
 */
class WorkBuffer implements AutoCloseable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkBuffer.class);

    private static final AtomicInteger cnt = new AtomicInteger(0);

    private final ValueVector[] vectors;
    private final BaseWriter.ListWriter[] listWriters;
    private final BufferAllocator allocator;
    private final int batchSize;
    private int vectorDimension;

    public WorkBuffer(List<Field> fields, BufferAllocator parent, long allocationLimit, int batchSize) {
        this.vectorDimension = 0;
        this.allocator = parent.newChildAllocator(
                String.format("work-buffer-%d", cnt.getAndIncrement()),
                0L, allocationLimit);

        this.vectors = fields.stream()
                .map(field -> field.createVector(this.allocator))
                .collect(Collectors.toUnmodifiableList())
                .toArray(ValueVector[]::new);

        this.listWriters = new BaseWriter.ListWriter[this.vectors.length];
        this.batchSize = batchSize;
    }

    public void init() {
        vectorDimension = 0;

        IntStream.range(0, vectors.length)
                .forEach(idx -> {
                    final ValueVector vector = vectors[idx];
                    vector.setInitialCapacity(batchSize);
                    int retries = 10_000;
                    while (!vector.allocateNewSafe()) {
                        --retries;
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            retries = 0;
                            break;
                        }
                    }
                    if (retries == 0) {
                        throw new RuntimeException("failed to allocate memory for work buffer");
                    }

                    BaseWriter.ListWriter writer = null;
                    if (vector instanceof FixedSizeListVector) {
                        writer = ((FixedSizeListVector) vector).getWriter();
                    } else if (vector instanceof ListVector) {
                        writer = ((ListVector) vector).getWriter();
                        ((UnionListWriter) writer).start();
                    }

                    listWriters[idx] = writer;
                    vectors[idx] = vector;
                });
    }

    public int convert(RowBasedRecord row) {
        for (int n = 0; n < vectors.length; n++) {
            convertValue(vectorDimension, row.get(n), vectors[n], listWriters[n]);
        }
        vectorDimension++;
        return vectorDimension;
    }

    protected void convertValue(int idx, RowBasedRecord.Value value, ValueVector vector, BaseWriter.ListWriter writer) {
        if (vector instanceof IntVector) {
            ((IntVector) vector).set(idx, value.asInt());
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).set(idx, value.asLong());
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).set(idx, value.asFloat());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).set(idx, value.asDouble());
        } else if (vector instanceof VarCharVector && value.asString() != null) {
            ((VarCharVector) vector).setSafe(idx, value.asString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof FixedSizeListVector && writer instanceof UnionFixedSizeListWriter) {
            // XXX: Assumes all values share the same type and first value is non-null
            final UnionFixedSizeListWriter listWriter = (UnionFixedSizeListWriter) writer;
            listWriter.startList();
            switch (value.type()) {
                case INT_ARRAY:
                    for (int i : value.asIntArray())
                        listWriter.writeInt(i);
                    break;
                case LONG_ARRAY:
                    for (long l : value.asLongArray())
                        listWriter.writeBigInt(l);
                    break;
                case FLOAT_ARRAY:
                    for (float f : value.asFloatArray())
                        listWriter.writeFloat4(f);
                    break;
                case DOUBLE_ARRAY:
                    for (double d : value.asDoubleArray())
                        listWriter.writeFloat8(d);
                    break;
                default:
                    // TODO: abort
                    throw new RuntimeException("unsupported fixed size list value type: " + value.type());
            }
            listWriter.setValueCount(value.size());
            listWriter.endList();
        } else if (vector instanceof ListVector && writer instanceof UnionListWriter) {
            final UnionListWriter listWriter = (UnionListWriter) writer;
            listWriter.startList();
            switch (value.type()) {
                case INT_ARRAY:
                    for (int i : value.asIntArray())
                        listWriter.writeInt(i);
                    break;
                case LONG_ARRAY:
                    for (long l : value.asLongArray())
                        listWriter.writeBigInt(l);
                    break;
                case FLOAT_ARRAY:
                    for (float f : value.asFloatArray())
                        listWriter.writeFloat4(f);
                    break;
                case DOUBLE_ARRAY:
                    for (double d : value.asDoubleArray())
                        listWriter.writeFloat8(d);
                    break;
                case INT_LIST:
                    /* XXX: for now we'll try using an int array instead of a List<Integer> for the value */
                    try {
                        for (int i : value.asIntArray()) {
                            listWriter.writeInt(i);
                        }
                    } catch (OutOfMemoryException oom) {
                        logger.error(String.format("OOM writing INT_LIST %s (value size: %,d, allocator limit: %,d): %s",
                                vector.getName(), value.size(), allocator.getLimit(), oom.getMessage()));
                        throw oom;
                    }
                    break;
                case LONG_LIST:
                    try {
                        for (long l : value.asLongList()) {
                            listWriter.writeBigInt(l);
                        }
                    } catch (OutOfMemoryException oom) {
                        logger.error(String.format("OOM writing LONG_LIST %s (value size: %,d, allocator limit: %,d): %s",
                                vector.getName(), value.size(), allocator.getLimit(), oom.getMessage()));
                        throw oom;
                    }
                    break;
                case STRING_LIST:
                    for (final String s : value.asStringList()) {
                        // TODO: should we allocate a single byte array and not have to reallocate?
                        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                        try (final ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.setBytes(0, bytes);
                            listWriter.writeVarChar(0, bytes.length, buf);
                            logger.trace("wrote string {}", s);
                        }
                    }
                    break;
                default:
                    for (final Object o : value.asList()) {
                        // TODO: should we allocate a single byte array and not have to reallocate?
                        final byte[] bytes = o.toString().getBytes(StandardCharsets.UTF_8);
                        try (final ArrowBuf buf = allocator.buffer(bytes.length)) {
                            buf.setBytes(0, bytes);
                            listWriter.writeVarChar(0, bytes.length, buf);
                        }
                    }
                    break;
            }
            writer.endList();
        }
    }

    public void prepareForFlush() {
        assert(vectorDimension > 0);

        for (BaseWriter.ListWriter writer : listWriters) {
            if (writer instanceof UnionListWriter) {
                ((UnionListWriter) writer).end();
            }
        }

        for (final ValueVector vector : vectors) {
            vector.setValueCount(vectorDimension);
            if (vector instanceof ListVector) {
                ((ListVector) vector).setLastSet(vectorDimension - 1);
            }
        }
    }

    public List<ValueVector> transfer(BufferAllocator toAllocator) {
        final List<ValueVector> list = new ArrayList<>();

        for (int i=0; i<vectors.length; i++) {
            final ValueVector vector = vectors[i];
            final TransferPair tp = vector.getTransferPair(toAllocator);
            tp.transfer();
            list.add(tp.getTo());
            vector.clear();

            final BaseWriter.ListWriter writer = listWriters[i];
            if (writer != null) AutoCloseables.closeNoChecked(writer);
        }

        try {
            for (BaseWriter.ListWriter writer : listWriters) {
                if (writer != null) writer.close();
            }
        } catch (Exception e) {
            logger.warn("error while closing writer: " + e.getMessage());
        }

        return list;
    }

    public void release() {
        Arrays.stream(vectors).forEach(ValueVector::close);
    }

    public int getVectorDimension() {
        return vectorDimension;
    }

    public List<ValueVector> getVectors() {
        return Arrays.stream(vectors).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void close() throws Exception {
        for (BaseWriter.ListWriter writer : listWriters) {
            if (writer != null) {
                writer.close();
            }
        }

        for (ValueVector vector : vectors) {
            vector.close();
        }

        assert(allocator.getAllocatedMemory() == 0);
        allocator.close();
        logger.debug("closed workbuffer {}", allocator.getName());
    }
}
