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

    private final long origin;
    private final List<Long> sourceIds;
    private final List<Long> targetIds;


    protected SubGraphRecord(long origin, List<Long> sourceIds, List<Long> targetIds) {
        this.origin = origin;
        this.sourceIds = sourceIds;
        this.targetIds = targetIds;
    }

    public static SubGraphRecord of(long origin, List<Long> sourceIds, List<Long> targetIds) {
        if (sourceIds.size() != targetIds.size()) {
            throw new IllegalArgumentException("length of both source and target ids must be the same");
        }
        return new SubGraphRecord(origin, sourceIds, targetIds);
    }

    public static SubGraphRecord of(long origin, Iterable<Long> edges) {
        final List<Long> sources = new ArrayList<>();
        final List<Long> targets = new ArrayList<>();

        edges.forEach(edge -> {
            sources.add(Edge.source(edge));
            targets.add(Edge.target(edge));
        });
        return new SubGraphRecord(origin, sources, targets);
    }

    public static SubGraphRecord of(Node source, Relationship rel, Node target) {
        return null;
    }

    @Override
    public Value get(int index) {
        switch (index) {
            case 0: return GdsRecord.wrapScalar(origin, ValueType.LONG);
            case 1: return GdsRecord.wrapLongs(sourceIds);
            case 2: return GdsRecord.wrapLongs(targetIds);
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
