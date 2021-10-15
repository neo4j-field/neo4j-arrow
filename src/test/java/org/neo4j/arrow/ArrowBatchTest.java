package org.neo4j.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArrowBatchTest {

    @Test
    public void testSearchingTailEnd() {
        Field field = new Field("junk", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Schema schema = new Schema(List.of(field));

        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             ArrowBatch batch = new ArrowBatch(schema, allocator, "test-batch")) {

            long nodeId = 0;
            for (int i=0; i<10; i++) {
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    FieldVector vector = root.getVector(0);
                    vector.allocateNewSafe();
                    BigIntVector biv = (BigIntVector) vector;
                    for (int j=0; j<6; j++) {
                        biv.set(j, nodeId++);
                    }
                    root.setRowCount(6);
                    System.out.println("created vector: " + biv);

                    batch.appendRoot(root);
                    vector.close();
                }
            }

            for (int i=0; i<48; i++) {
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    FieldVector vector = root.getVector(0);
                    vector.allocateNewSafe();
                    BigIntVector biv = (BigIntVector) vector;
                    for (int j=0; j<5; j++) {
                        biv.set(j, nodeId++);
                    }
                    root.setRowCount(5);
                    System.out.println("created vector: " + biv);

                    batch.appendRoot(root);
                    vector.close();
                }
            }

            ArrowBatch.BatchedVector bv = batch.getVector(0);
            List<ValueVector> list = bv.getVectors();
            Assertions.assertEquals(58, list.size());

            Set<Long> seen = new HashSet<>();
            for (long i=0; i<300; i++) {
                final long id = bv.getNodeId(i);
                System.out.println("i=" + i + ", nodeId= " + id);
                if (seen.contains(id)) {
                    Assertions.fail("already seen nodeId: " + id);
                }
                seen.add(id);
            }
        }
    }
}
