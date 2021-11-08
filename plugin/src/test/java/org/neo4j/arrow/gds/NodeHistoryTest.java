package org.neo4j.arrow.gds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeHistoryTest {
    @Test
    public void testSmallEdgeCache() {
        NodeHistory small = NodeHistory.given(3, 4);
        int node1 = 1;
        int node2 = 2;
        int node3 = 2;
        int node4 = 3;

        Assertions.assertFalse(small.getAndSet(node1));
        Assertions.assertFalse(small.getAndSet(node2));
        Assertions.assertTrue(small.getAndSet(node3));
        Assertions.assertFalse(small.getAndSet(node4));
    }

    @Test
    public void testHugeEdgeCache() {
        NodeHistory small = NodeHistory.given(3_000_000, 3_000_000);

        int node1 = 1;
        int node2 = 2;
        int node3 = 2;
        int node4 = 3;

        Assertions.assertFalse(small.getAndSet(node1));
        Assertions.assertFalse(small.getAndSet(node2));
        Assertions.assertTrue(small.getAndSet(node3));
        Assertions.assertFalse(small.getAndSet(node4));
    }

    @Test
    public void testOffHeapEdgeCache() {
        NodeHistory offheap = NodeHistory.offHeap(200);
        int node1 = 42;
        Assertions.assertFalse(offheap.getAndSet(node1));
        Assertions.assertTrue(offheap.getAndSet(node1));
        Assertions.assertFalse(offheap.getAndSet(node1 + 1));
        Assertions.assertFalse(offheap.getAndSet(node1 - 1));

        Assertions.assertFalse(offheap.getAndSet(0));
        Assertions.assertFalse(offheap.getAndSet(199));
        Assertions.assertThrows(IndexOutOfBoundsException.class,
                () -> offheap.getAndSet(200));
        Assertions.assertThrows(IndexOutOfBoundsException.class,
                () -> offheap.getAndSet(-1));
    }
}
