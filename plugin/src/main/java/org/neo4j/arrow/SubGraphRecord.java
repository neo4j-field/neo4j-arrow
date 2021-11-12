package org.neo4j.arrow;

import org.neo4j.arrow.gds.Edge;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SubGraphRecord implements RowBasedRecord {

    public final static String KEY_ORIGIN_ID = "_origin_id_";
    public final static String KEY_SOURCE_IDS = "_source_ids_";
    public final static String KEY_TARGET_IDS = "_target_ids_";

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

    public static SubGraphRecord of(long origin, Iterable<Long> edges, int size) {
        final int[] sources = new int[size];
        final int[] targets = new int[size];

        int pos = 0;
        for (long edge : edges) {
            sources[pos] = Edge.sourceAsInt(edge);
            targets[pos] = Edge.targetAsInt(edge);
            pos++;
        }
        return new SubGraphRecord((int) origin, sources, targets); // XXX cast
    }

    public static SubGraphRecord of(Node source, Relationship rel, Node target) {
        return null;
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
        switch (field.toLowerCase()) {
            case KEY_ORIGIN_ID:  return get(0);
            case KEY_SOURCE_IDS: return get(1);
            case KEY_TARGET_IDS: return get(2);
            default:
                throw new RuntimeException("invalid field: " + field);
        }
    }

    public int numEdges() {
        return sourceIds.length;
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
