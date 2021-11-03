package org.neo4j.arrow.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EdgePackingTest {
    @Test
    public void testEdgePackingLogic() {
        long startId = 123;
        long endId = 456;
        boolean isNatural = true;

        long edge = GdsReadJob.edge(startId, endId, isNatural);
        System.out.printf("0x%X\n", edge);
        System.out.printf("start: %d ?? %d\n", startId, GdsReadJob.source(edge));
        Assertions.assertEquals(startId, GdsReadJob.source(edge));

        System.out.printf("end: %d ?? %d\n", endId, GdsReadJob.target(edge));
        Assertions.assertEquals(endId, GdsReadJob.target(edge));

        System.out.printf("isNatural?: %s : %s\n", isNatural, GdsReadJob.flag(edge));
        Assertions.assertEquals(isNatural, GdsReadJob.flag(edge));

        startId = 300_000_000;
        endId = 0;
        isNatural = false;
        edge = GdsReadJob.edge(startId, endId, isNatural);
        System.out.printf("0x%X\n", edge);
        System.out.printf("start: %d ?? %d\n", startId, GdsReadJob.source(edge));
        Assertions.assertEquals(startId, GdsReadJob.source(edge));

        System.out.printf("end: %d ?? %d\n", endId, GdsReadJob.target(edge));
        Assertions.assertEquals(endId, GdsReadJob.target(edge));

        System.out.printf("isNatural?: %s : %s\n", isNatural, GdsReadJob.flag(edge));
        Assertions.assertEquals(isNatural, GdsReadJob.flag(edge));
    }
}
