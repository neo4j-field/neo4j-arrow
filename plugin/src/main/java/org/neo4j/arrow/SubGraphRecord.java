package org.neo4j.arrow;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.*;
import java.util.stream.Collectors;

public class SubGraphRecord implements RowBasedRecord {

    public final static String KEY_ORIGIN_ID = "_origin_id_";
    public final static String KEY_SOURCE_ID = "_source_id_";
    public final static String KEY_SOURCE_LABELS = "_source_labels_";
    public final static String KEY_REL_ID = "_rel_id_";
    public final static String KEY_REL_TYPE = "_rel_type_";
    public final static String KEY_TARGET_ID = "_target_id_";
    public final static String KEY_TARGET_LABELS = "_target_labels_";

    private final String[] keys = {
            KEY_ORIGIN_ID,
            KEY_SOURCE_ID, KEY_SOURCE_LABELS,
            KEY_REL_ID, KEY_REL_TYPE,
            KEY_TARGET_ID, KEY_TARGET_LABELS };

    private final long origin;
    private final long source;
    private final Set<NodeLabel> sourceLabels;
    private final String relType;
    private final long relId;
    private final long target;
    private final Set<NodeLabel> targetLabels;


    public SubGraphRecord(long origin, long source, Set<NodeLabel> sourceLabels, long relId, String type, long target, Set<NodeLabel> targetLabels) {
        this.origin = origin;
        this.source = source;
        this.sourceLabels = sourceLabels;
        this.relType = type;
        this.relId = relId;
        this.target = target;
        this.targetLabels = targetLabels;
    }

    public static SubGraphRecord of(long origin,
                                    long source, Set<NodeLabel> sourceLabels,
                                    long relId, String relType,
                                    long target, Set<NodeLabel> targetLabels) {
        return new SubGraphRecord(origin, source, sourceLabels, relId, relType, target, targetLabels);
    }

    public static SubGraphRecord of(Node source, Relationship rel, Node target) {
        final Set<NodeLabel> sourceLabels = new HashSet<>();
        source.getLabels().forEach(lbl -> sourceLabels.add(NodeLabel.of(lbl.name())));

        final Set<NodeLabel> targetLabels = new HashSet<>();
        target.getLabels().forEach(lbl -> targetLabels.add(NodeLabel.of(lbl.name())));

        /*
        return new SubGraphRecord(source.getId(), sourceLabels,
                rel.getId(), rel.getType().toString(),
                target.getId(), targetLabels);

         */
        return null;
    }

    @Override
    public Value get(int index) {
        switch (index) {
            case 0: return GdsRecord.wrapScalar(origin, ValueType.LONG);
            case 1: return GdsRecord.wrapScalar(source, ValueType.LONG);
            case 2: return GdsNodeRecord.wrapNodeLabels(sourceLabels);
            case 3: return GdsRecord.wrapScalar(relId, ValueType.LONG);
            case 4: return GdsRecord.wrapString(relType);
            case 5: return GdsRecord.wrapScalar(target, ValueType.LONG);
            case 6: return GdsNodeRecord.wrapNodeLabels(targetLabels);
            default:
                throw new RuntimeException("invalid index: " + index);
        }
    }

    @Override
    public Value get(String field) {
        switch (field.toLowerCase()) {
            case KEY_ORIGIN_ID:  return get(0);
            case KEY_SOURCE_ID: return get(1);
            case KEY_SOURCE_LABELS: return get(2);
            case KEY_REL_ID: return get(3);
            case KEY_REL_TYPE: return get(4);
            case KEY_TARGET_ID: return get(5);
            case KEY_TARGET_LABELS: return get(6);
            default:
                throw new RuntimeException("invalid field: " + field);
        }
    }

    @Override
    public List<String> keys() {
        return Arrays.stream(keys).collect(Collectors.toList());
    }
}
