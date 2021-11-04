package org.neo4j.arrow.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.arrow.gds.Edge;

public class EdgePackingTest {
    @Test
    public void testEdgePackingLogic() {
        long startId = 123;
        long endId = 456;
        boolean isNatural = true;

        long edge = Edge.edge(startId, endId, isNatural);
        System.out.printf("0x%X\n", edge);
        System.out.printf("start: %d ?? %d\n", startId, Edge.source(edge));
        Assertions.assertEquals(startId, Edge.source(edge));

        System.out.printf("end: %d ?? %d\n", endId, Edge.target(edge));
        Assertions.assertEquals(endId, Edge.target(edge));

        System.out.printf("isNatural?: %s : %s\n", isNatural, Edge.flag(edge));
        Assertions.assertEquals(isNatural, Edge.flag(edge));

        startId = 300_000_000;
        endId = 0;
        isNatural = false;
        edge = Edge.edge(startId, endId, isNatural);
        System.out.printf("0x%X\n", edge);
        System.out.printf("start: %d ?? %d\n", startId, Edge.source(edge));
        Assertions.assertEquals(startId, Edge.source(edge));

        System.out.printf("end: %d ?? %d\n", endId, Edge.target(edge));
        Assertions.assertEquals(endId, Edge.target(edge));

        System.out.printf("isNatural?: %s : %s\n", isNatural, Edge.flag(edge));
        Assertions.assertEquals(isNatural, Edge.flag(edge));
    }
}
