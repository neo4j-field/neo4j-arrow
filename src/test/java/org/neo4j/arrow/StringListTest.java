package org.neo4j.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("all")
public class StringListTest {

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

        try (BufferAllocator allocator = new RootAllocator(Config.maxGlobalMemory);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            List<ArrowBuf> buffers = new ArrayList<>();

            FieldVector fv = field.createVector(allocator);
            assert(fv instanceof ListVector);
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
}
