package org.neo4j.arrow;

import org.neo4j.arrow.gds.Edge;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SubGraphRecord implements RowBasedRecord {

    public final static String KEY_ORIGIN_ID = Neo4jDefaults.ID_FIELD;
    public final static String KEY_SOURCE_IDS = Neo4jDefaults.SOURCE_FIELD;
    public final static String KEY_TARGET_IDS = Neo4jDefaults.TARGET_FIELD;

    private final String[] keys = {
            KEY_ORIGIN_ID,
            KEY_SOURCE_IDS,
            KEY_TARGET_IDS
    };

    private final int origin;
    private final int[] sourceIds;
    private final int[] targetIds;


    protected SubGraphRecord(int origin, int[] sourceIds, int[] targetIds) {
        this.origin = origin;
        this.sourceIds = sourceIds;
        this.targetIds = targetIds;
    }

    public static SubGraphRecord of(int origin, int[] sourceIds, int[] targetIds) {
        if (sourceIds.length != targetIds.length) {
            throw new IllegalArgumentException("length of both source and target id arrays must be the same");
        }
        return new SubGraphRecord(origin, sourceIds, targetIds);
    }

    public static SubGraphRecord of(long origin, Iterable<Long> edges, int size, Function<Long, Long> idMapper) {
        final int[] sources = new int[size];
        final int[] targets = new int[size];

        int pos = 0;
        for (long edge : edges) {
            sources[pos] = idMapper.apply(Edge.source(edge)).intValue();
            targets[pos] = idMapper.apply(Edge.target(edge)).intValue();
            pos++;
        }
        return new SubGraphRecord(idMapper.apply(origin).intValue(), sources, targets); // XXX cast
    }

    @Override
    public Value get(int index) {
        switch (index) {
            case 0: return GdsRecord.wrapInt(origin);
            case 1: return GdsRecord.wrapInts(sourceIds);
            case 2: return GdsRecord.wrapInts(targetIds);
            default:
                throw new RuntimeException("invalid index: " + index);
        }
    }

    @Override
    public Value get(String field) {
        switch (field) {
            case KEY_ORIGIN_ID:  return get(0);
            case KEY_SOURCE_IDS: return get(1);
            case KEY_TARGET_IDS: return get(2);
            default:
                throw new RuntimeException("invalid field: " + field);
        }
    }

    @Override
    public String toString() {
        return "SubGraphRecord{" +
                "origin=" + origin +
                ", sourceIds=" + sourceIds +
                ", targetIds=" + targetIds +
                '}';
    }

    @Override
    public List<String> keys() {
        return Arrays.stream(keys).collect(Collectors.toList());
    }
}
