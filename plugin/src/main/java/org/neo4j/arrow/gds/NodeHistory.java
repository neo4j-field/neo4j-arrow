package org.neo4j.arrow.gds;

import org.neo4j.gds.core.utils.BitUtil;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public abstract class NodeHistory {
    public static final int CUTOFF = 1_000_000;

    public static NodeHistory given(int numNodes) {
        assert(numNodes < (1 << 30));
        if (numNodes < CUTOFF) {
            return new SmolNodeHistory();
        }
        return new LorgeNodeHistory();
    }

    public static NodeHistory offHeap(int numNodes) {
        return new OffHeapNodeHistory(numNodes);
    }

    public abstract boolean getAndSet(int node);

    protected static class SmolNodeHistory extends NodeHistory {
        // XXX not threadsafe

        private final Set<Integer> set = new HashSet<>();

        @Override
        public boolean getAndSet(int node) {
            return !set.add(node);
        }
    }

    protected static class OffHeapNodeHistory extends NodeHistory {
        private final ByteBuffer buffer;
        private final int size;

        protected OffHeapNodeHistory(int size) {
            this.size = size;
            buffer = ByteBuffer.allocateDirect(BitUtil.ceilDiv(size, Byte.SIZE));
        }

        @Override
        public boolean getAndSet(int node) {
            if (0 > node || node >= size)
                throw new IndexOutOfBoundsException(String.format("%d out of bounds for history of size %d", node, size));

            boolean result;
            final int index = Math.floorDiv(node, Byte.SIZE);
            final int bitMask = 1 << Math.floorMod(node, Byte.SIZE);
            synchronized (buffer) {
                final byte b = buffer.get(index);
                result = (b & bitMask) != 0;
                buffer.put(index, (byte) (b | bitMask));
            }
            return result;
        }
    }

    protected static class LorgeNodeHistory extends NodeHistory {

        private final RoaringBitmap bitmap = new RoaringBitmap();

        @Override
        public boolean getAndSet(int node) {
            // XXX not threadsafe
            final boolean result = bitmap.contains(node);
            bitmap.add(node);
            return result;
        }
    }
}
