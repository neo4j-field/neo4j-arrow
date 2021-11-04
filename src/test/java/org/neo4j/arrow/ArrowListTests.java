package org.neo4j.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

@SuppressWarnings("all")
public class ArrowListTests {

    @Test
    public void longListTest() {
        try (BufferAllocator allocator = new RootAllocator(((5 << 20)))) {
            Field field = new Field("test-list", FieldType.nullable(new ArrowType.List()),
                List.of(new Field("test-data",
                        FieldType.nullable(new ArrowType.Int(64, true)),
                        null)));
            final List<ArrowBuf> buffers = new ArrayList<>();

            FieldVector fv = field.createVector(allocator);
            System.out.println("building fv named " + fv.getName());
            ((ListVector)fv).setInitialCapacity(1024, 10);
            fv.allocateNewSafe();
            UnionListWriter writer = ((ListVector)fv).getWriter();
            writer.start();
            int i=0;
            for (i=0; i<4048; i++) {
                writer.startList();
                for (int j=i; j<5000; j++) {
                    writer.writeBigInt(j);
                }
                writer.endList();
            }
            writer.end();
            fv.setValueCount(i+1);
            ((ListVector) fv).setLastSet(i);

            System.out.println(fv);

            buffers.add(fv.getValidityBuffer());
            buffers.add(fv.getOffsetBuffer());

            for (FieldVector child : ((BaseListVector)fv).getChildrenFromFields()) {
                final UnionVector uv = (UnionVector) child;

                switch (field.getChildren().get(0).getType().getTypeID()) {
                    case Int:
                        System.out.println("int list?");
                        System.out.println(uv.getBigIntVector());
                        break;
                    default:
                        break;
                }

                System.out.println("uv field type: " + uv.getField().getFieldType().getType().getTypeID());
                System.out.println("uv field: " + uv.getField().toString());
                System.out.println("field: " + field);
                System.out.println("field2: " + field.getChildren().get(0).toString());

                System.out.println(uv.getClass().getCanonicalName());
                System.out.println("child: " + child);
                for (FieldVector grandchild : uv.getChildrenFromFields()) {
                    System.out.println(grandchild.getClass().getCanonicalName());
                    System.out.println("grandchild: " + grandchild);
                    System.out.println(grandchild.getBuffers(false));
                }
            }

            System.out.println(buffers);
            fv.close();
        }
    }

    @Test
    public void vectorAppendTest() {
        try (BufferAllocator allocator = new RootAllocator((5 << 20))) {
            IntVector v1 = new IntVector("v1", allocator);

            v1.allocateNew(1024);

            for (int i = 0; i < 10; i++) {
                IntVector v2 = new IntVector("v2", allocator);
                v2.allocateNew(10);
                for (int j = 1; j <= 10; j++)
                    v2.setSafe(j - 1, (i * 10) + j);
                v2.setValueCount(10);

                VectorBatchAppender.batchAppend(v1, v2);

                v2.close();
                v1.setValueCount((i * 10));
            }

            System.out.println(v1);
            v1.close();
        }
    }

    @Test
    public void batchSizeTest() {
        long rowCount = 6000;
        long batchSize = 750;
        for (long l = 0; l < rowCount; l += batchSize) {
            long start = l;
            long finish = Math.min(l + batchSize, rowCount);
            System.out.println(String.format("%d -> %d", start, finish));
        }
    }

    @Test
    public void testStringListCreation() throws Exception {
        Field field = new Field("test-list", FieldType.nullable(new ArrowType.List()),
                List.of(new Field("test-data",
                        FieldType.nullable(new ArrowType.Utf8()),
                        null)));
        Schema schema = new Schema(List.of(field));

        try (BufferAllocator allocator = new RootAllocator((5 << 20));
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            List<ArrowBuf> buffers = new ArrayList<>();

            FieldVector fv = field.createVector(allocator);
            assert (fv instanceof ListVector);
            ListVector vector = ((ListVector) fv);
            vector.allocateNewSafe();
            UnionListWriter writer = vector.getWriter();
            writer.setPosition(0);

            ArrowBuf buf = allocator.buffer(24);
            ArrowBuf buf2 = allocator.buffer(24);

            buf.writeBytes("Hey".getBytes(StandardCharsets.UTF_8));
            buf2.writeBytes("Sup!".getBytes(StandardCharsets.UTF_8));
            writer.start();
            writer.startList();
            writer.writeVarChar(0, 3, buf);
            writer.writeVarChar(0, 3, buf);
            writer.endList();
            System.out.println(vector);

            writer.startList();
            writer.writeVarChar(0, 3, buf);
            writer.endList();
            System.out.println(vector);

            writer.end();
            System.out.println(vector);

            vector.setValueCount(2);
            vector.setLastSet(1);
            buf2.close();
            buf.close();
            System.out.println(vector);

            System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

            VectorLoader loader = new VectorLoader(root);
            List<ArrowFieldNode> nodes = new ArrayList<>();
            ArrowFieldNode node = new ArrowFieldNode(2, 0);
            nodes.add(node);

            buffers.add(vector.getValidityBuffer());
            buffers.add(vector.getOffsetBuffer());

            System.out.println("children:");
            FieldVector child = vector.getChildrenFromFields().get(0);
            if (child instanceof UnionVector) {
                nodes.add(new ArrowFieldNode(child.getValueCount(), child.getNullCount()));
                UnionVector uv = (UnionVector) child;

                System.out.println("child: " + child.getClass().getCanonicalName());

                if (uv.getChildrenFromFields().size() > 0) {
                    for (FieldVector grandchild : uv.getChildrenFromFields()) {
                        //nodes.add(new ArrowFieldNode(grandchild.getValueCount(), grandchild.getNullCount()));

                        if (grandchild instanceof VarCharVector) {
                            VarCharVector vcv = (VarCharVector) grandchild;
                            buffers.add(vcv.getValidityBuffer());
                            buffers.add(vcv.getOffsetBuffer());
                            buffers.add(vcv.getDataBuffer());
                        } else if (grandchild instanceof StructVector) {
                            StructVector sv = (StructVector) grandchild;
                            System.out.println("sv: cnt=" + sv.getValueCount() + ", sv = " + sv.toString());
                            //buffers.add(sv.getValidityBuffer());
                        }

                    }
                }
            }


            // buffers.add(((UnionVector)(vector.getChildrenFromFields()).get(0)).getTypeBuffer());
            try (ArrowRecordBatch batch = new ArrowRecordBatch(2, nodes, buffers)) {
                loader.load(batch);
            }
            vector.close();
        }
    }

    @Test
    public void testMemoryUsage() {
        try (BufferAllocator allocator = new RootAllocator(1 << 20)) {
            try (UInt4Vector vector = new UInt4Vector("test", allocator);
                 UInt4Vector source = new UInt4Vector("source", allocator)) {

                source.allocateNew(64);
                for (int i = 0; i < 64; i++) {
                    source.set(i, i);
                }
                source.setValueCount(64);
                System.out.printf("source capacity: %,d\n", source.getValueCapacity());
                System.out.printf("vector capacity: %,d\n", vector.getValueCapacity());

                System.out.printf("source valcount: %,d\n", source.getValueCount());
                System.out.printf("vector valcount: %,d\n", vector.getValueCount());

                System.out.printf("source buf size: %,d\n", source.getBufferSize());
                System.out.printf("vector buf size: %,d\n", vector.getBufferSize());

                System.out.printf("allocator: %,d\n", allocator.getAllocatedMemory());

                source.transferTo(vector);
                System.out.println("------xfer ------");

                System.out.printf("source capacity: %,d\n", source.getValueCapacity());
                System.out.printf("vector capacity: %,d\n", vector.getValueCapacity());

                System.out.printf("source valcount: %,d\n", source.getValueCount());
                System.out.printf("vector valcount: %,d\n", vector.getValueCount());

                System.out.printf("source buf size: %,d\n", source.getBufferSize());
                System.out.printf("vector buf size: %,d\n", vector.getBufferSize());

                System.out.printf("allocator: %,d\n", allocator.getAllocatedMemory());

                System.out.println("------close? ------");
                vector.close();

                System.out.printf("source capacity: %,d\n", source.getValueCapacity());
                System.out.printf("vector capacity: %,d\n", vector.getValueCapacity());

                System.out.printf("source valcount: %,d\n", source.getValueCount());
                System.out.printf("vector valcount: %,d\n", vector.getValueCount());

                System.out.printf("source buf size: %,d\n", source.getBufferSize());
                System.out.printf("vector buf size: %,d\n", vector.getBufferSize());
                System.out.printf("allocator: %,d\n", allocator.getAllocatedMemory());

            }
        }
    }

    @Test
    public void testVectorSchemaRootMemory() {

        AllocationListener listener = new AllocationListener() {

            @Override
            public void onAllocation(long size) {
                System.out.println("ALLOC(" + size + ")");
            }

            @Override
            public void onRelease(long size) {
                System.out.println("FREE(" + size + ")");
            }
        };

        try (BufferAllocator allocator = new RootAllocator(listener, 1 << 20)) {
            Field field = new Field("test",
                    FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schema = new Schema(List.of(field));

            FieldVector fv = field.createVector(allocator);
            fv.setInitialCapacity(12);
            fv.allocateNew();
            for (int i = 0; i < 12; i++) {
                ((IntVector) fv).set(i, i);
            }
            fv.setValueCount(12);
            System.out.println(fv);
            System.out.printf("fv: %,d\n", fv.getBufferSize());

            try (VectorSchemaRoot root = VectorSchemaRoot.of(fv);
                 VectorSchemaRoot sink = VectorSchemaRoot.create(schema, allocator)) {
                VectorLoader loader = new VectorLoader(sink);
                VectorUnloader unloader = new VectorUnloader(root);

                System.out.printf("root: %,d\n", root.getRowCount());
                System.out.printf("sink: %,d\n", sink.getRowCount());
                System.out.printf("fv: %,d\n", fv.getBufferSize());
                System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    System.out.println("batch : " + batch);

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv: %,d\n", fv.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                    loader.load(batch);
                    System.out.println("loaded...");

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv: %,d\n", fv.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                    root.clear();

                    System.out.println("cleared root...");

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv: %,d\n", fv.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                    sink.clear();

                    System.out.println("cleared sink...");

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv: %,d\n", fv.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());
                }

                fv.close();
                System.out.println("fv closed...");
                System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());


                FieldVector fv2 = field.createVector(allocator);
                fv2.setInitialCapacity(12);
                fv2.allocateNew();
                for (int i = 0; i < 12; i++) {
                    ((IntVector) fv2).set(i, i);
                }
                fv2.setValueCount(12);
                System.out.println(fv2);
                System.out.printf("fv2: %,d\n", fv2.getBufferSize());

                ArrowRecordBatch batch = new ArrowRecordBatch(12,
                        List.of(new ArrowFieldNode(12, 0)),
                        List.of(fv2.getBuffers(false)));
                VectorLoader rootLoader = new VectorLoader(root);
                rootLoader.load(batch);
                batch.close();

                System.out.println("added fv2 to root via batch...");

                System.out.printf("root: %,d\n", root.getRowCount());
                System.out.printf("sink: %,d\n", sink.getRowCount());
                System.out.printf("fv: %,d\n", fv.getBufferSize());
                System.out.printf("fv2 %,d\n", fv2.getBufferSize());
                System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());
                fv2.close();
                System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                try (ArrowRecordBatch batch2 = unloader.getRecordBatch()) {
                    System.out.println("batch : " + batch2);

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv2: %,d\n", fv2.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                    loader.load(batch2);
                    System.out.println("loaded...");

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv2: %,d\n", fv2.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                    root.clear();

                    System.out.println("cleared root...");

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv2: %,d\n", fv2.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                    sink.clear();

                    System.out.println("cleared sink...");

                    System.out.printf("root: %,d\n", root.getRowCount());
                    System.out.printf("sink: %,d\n", sink.getRowCount());
                    System.out.printf("fv2: %,d\n", fv2.getBufferSize());
                    System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());
                }
                fv.clear();
                fv2.clear();


                System.out.println("cleared fv{,2}");
                System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());

                root.getFieldVectors().forEach(FieldVector::close);
                sink.getFieldVectors().forEach(FieldVector::close);


                System.out.println("closed all via roots fv{,2}");
                System.out.printf("alloc: %,d\n", allocator.getAllocatedMemory());


            }

            System.out.printf("FINAL ALLOC: %,d\n", allocator.getAllocatedMemory());

        }
    }

    @Test
    public void testBufferGrowth() {
        final int dim = 2;
        final BufferAllocator rootAllocator = new RootAllocator(1024 * 1024 * 1024);
        final Map<String, BiFunction<BufferAllocator, Integer, FieldVector>> fnMap = new ConcurrentHashMap<>();

        fnMap.put("scalar", (allocator, capacity) -> {
            final Field field = new Field("scalar",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null);
            final Float8Vector sourceVector = (Float8Vector) field.createVector(allocator);
            sourceVector.setInitialCapacity(capacity);
            sourceVector.allocateNew(capacity);
            for (int i = 0; i < capacity; i++) {
                sourceVector.set(i, Integer.valueOf(i).doubleValue());
            }
            sourceVector.setValueCount(capacity);
            return sourceVector;
        });

        // populate
        fnMap.put("fixedList256", (allocator, capacity) -> {
            final Field field = new Field("embedding",
                    FieldType.nullable(new ArrowType.FixedSizeList(256)),
                    List.of(new Field("embedding-data",
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
            final FixedSizeListVector sourceVector = (FixedSizeListVector) field.createVector(allocator);
            sourceVector.setInitialCapacity(capacity);
            sourceVector.allocateNew();
            final UnionFixedSizeListWriter writer = sourceVector.getWriter();
            writer.start();
            for (int i = 0; i < capacity; i++) {
                writer.startList();
                for (int j = 0; j < dim; j++) {
                    writer.writeFloat8(Integer.valueOf(j).doubleValue());
                }
                writer.endList();
            }
            writer.end();
            sourceVector.setValueCount(capacity);

            return sourceVector;
        });

        fnMap.put("list256", (allocator, capacity) -> {
            final Field field = new Field("embedding",
                    FieldType.nullable(new ArrowType.List()),
                    List.of(new Field("embedding-data",
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
            final ListVector sourceVector = (ListVector) field.createVector(allocator);
            sourceVector.setInitialCapacity(capacity);
            try {
                sourceVector.allocateNew();
            } catch (Exception e) {
                Assertions.fail(e);
            }
            final UnionListWriter writer = sourceVector.getWriter();
            writer.start();
            for (int i = 0; i < capacity; i++) {
                writer.startList();
                for (int j = 0; j < dim; j++) {
                    writer.writeFloat8(Integer.valueOf(j).doubleValue());
                }
                writer.endList();
            }
            writer.end();
            sourceVector.setValueCount(capacity);

            return sourceVector;
        });

        final Map<Double, Integer> results = new ConcurrentHashMap<>();
        fnMap.forEach((name, fn) -> {
            IntStream.range(128, 4100).forEach(capacity -> {
                try (FieldVector sourceVector = fn.apply(rootAllocator, capacity)) {

                    final AtomicInteger sz = new AtomicInteger(0);
                    Arrays.stream(sourceVector.getBuffers(false)).forEach(buf -> sz.addAndGet((int) buf.capacity()));
                    results.put(sz.get() / (1.0d * dim * capacity), capacity);
                }
            });
            Double lowestRatio = results.keySet().stream().mapToDouble(Double::doubleValue).min().getAsDouble();
            Double highestRatio = results.keySet().stream().mapToDouble(Double::doubleValue).max().getAsDouble();

            System.out.printf("(%s) Lowest Ratio: %02f @ %,d capacity\n", name, lowestRatio, results.get(lowestRatio));
            System.out.printf("(%s) Highest Ratio: %02f @ %,d capacity\n", name, highestRatio, results.get(highestRatio));
            results.clear();
        });

        rootAllocator.close();
    }
}
