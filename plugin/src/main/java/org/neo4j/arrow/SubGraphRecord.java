package org.neo4j.arrow;

import org.neo4j.arrow.gds.Edge;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.*;
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
    private final List<Integer> sourceIds;
    private final List<Integer> targetIds;


    protected SubGraphRecord(int origin, List<Integer> sourceIds, List<Integer> targetIds) {
        this.origin = origin;
        this.sourceIds = sourceIds;
        this.targetIds = targetIds;
    }

    public static SubGraphRecord of(int origin, List<Integer> sourceIds, List<Integer> targetIds) {
        if (sourceIds.size() != targetIds.size()) {
            throw new IllegalArgumentException("length of both source and target ids must be the same");
        }
        return new SubGraphRecord(origin, sourceIds, targetIds);
    }

    public static SubGraphRecord of(long origin, Iterable<Long> edges) {
        final List<Integer> sources = new ArrayList<>();
        final List<Integer> targets = new ArrayList<>();

        edges.forEach(edge -> {
            sources.add(Edge.sourceAsInt(edge));
            targets.add(Edge.targetAsInt(edge));
        });
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
        return sourceIds.size();
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
