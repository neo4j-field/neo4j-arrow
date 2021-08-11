package org.neo4j.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.junit.jupiter.api.Test;

public class ListVectorTest {
    @Test
    public void testHowListVectorsWork() {
        try (BufferAllocator allocator = new RootAllocator()) {
            ListVector vector = ListVector.empty("test", allocator);
            vector.allocateNew();
            UnionListWriter writer = vector.getWriter();
            writer.startList();
            writer.writeUInt1((byte) 0xaa);
            writer.writeUInt1((byte) 0xbb);
            writer.writeUInt1((byte) 0xcc);
            writer.writeUInt1((byte) 0xdd);
            writer.setValueCount(4);
            writer.endList();
            writer.startList();
            writer.writeUInt1((byte) 0x33);
            writer.writeUInt1((byte) 0x44);
            writer.writeUInt1((byte) 0x55);
            writer.writeUInt1((byte) 0x66);
            writer.setValueCount(4);
            writer.endList();
            vector.setValueCount(2);
            vector.setLastSet(1);
            System.out.println(vector);

            FixedSizeListVector fslz = FixedSizeListVector.empty("test", 3, allocator);

            ArrowBuf buf = vector.getValidityBuffer();
            System.out.println("Validity Buffer:");
            for (long i=0; i<buf.readableBytes(); i++) {
                System.out.println(String.format("%d\t%02X", i, buf.getByte(i)));
            }

            buf = vector.getOffsetBuffer();
            System.out.println("Offset Buffer:");
            for (long i=0; i<buf.readableBytes(); i++) {
                System.out.println(String.format("%d\t%02X", i, buf.getByte(i)));
            }

            FieldVector child = vector.getDataVector();
            buf = child.getValidityBuffer();
            System.out.println("Child Validity Buffer:");
            for (long i=0; i<buf.readableBytes(); i++) {
                System.out.println(String.format("%d\t%02X", i, buf.getByte(i)));
            }

            buf = child.getDataBuffer();
            System.out.println("Data Buffer:");
            for (long i=0; i<buf.readableBytes(); i++) {
                System.out.println(String.format("%d\t%02X", i, buf.getByte(i)));
            }

        }
    }
}
