package org.neo4j.arrow;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.*;
import java.util.stream.Collectors;

public class SubGraphRecord implements RowBasedRecord {

    public final static String KEY_SOURCE_ID = "_source_id_";
    public final static String KEY_SOURCE_LABELS = "_source_labels_";
    public final static String KEY_REL_ID = "_rel_id_";
    public final static String KEY_REL_TYPE = "_rel_type_";
    public final static String KEY_TARGET_ID = "_target_id_";
    public final static String KEY_TARGET_LABELS = "_target_labels_";

    private final String[] keys = {
            KEY_SOURCE_ID, KEY_SOURCE_LABELS,
            KEY_REL_ID, KEY_REL_TYPE,
            KEY_TARGET_ID, KEY_TARGET_LABELS };

    private final long source;
    private final Set<NodeLabel> sourceLabels;
    private final String relType;
    private final long relId;
    private final long target;
    private final Set<NodeLabel> targetLabels;


    public SubGraphRecord(long source, Collection<NodeLabel> sourceLabels, long relId, String type, long target, Collection<NodeLabel> targetLabels) {
        this.source = source;
        this.sourceLabels = new HashSet<>(sourceLabels);
        this.relType = type;
        this.relId = relId;
        this.target = target;
        this.targetLabels = new HashSet<>(targetLabels);
    }

    public static SubGraphRecord of(Node source, Relationship rel, Node target) {
        final Set<NodeLabel> sourceLabels = new HashSet<>();
        source.getLabels().forEach(lbl -> sourceLabels.add(NodeLabel.of(lbl.name())));

        final Set<NodeLabel> targetLabels = new HashSet<>();
        target.getLabels().forEach(lbl -> targetLabels.add(NodeLabel.of(lbl.name())));

        return new SubGraphRecord(source.getId(), sourceLabels,
                rel.getId(), rel.getType().toString(),
                target.getId(), targetLabels);
    }

    @Override
    public Value get(int index) {
        switch (index) {
            case 0: return GdsRecord.wrapScalar(source, ValueType.LONG);
            case 1: return GdsNodeRecord.wrapNodeLabels(sourceLabels);
            case 2: return GdsRecord.wrapScalar(relId, ValueType.LONG);
            case 3: return GdsRecord.wrapString(relType);
            case 4: return GdsRecord.wrapScalar(target, ValueType.LONG);
            case 5: return GdsNodeRecord.wrapNodeLabels(targetLabels);
            default:
                throw new RuntimeException("invalid index");
        }
    }

    @Override
    public Value get(String field) {
        switch (field.toLowerCase()) {
            case KEY_SOURCE_ID: return GdsRecord.wrapScalar(source, ValueType.LONG);
            case KEY_SOURCE_LABELS: return GdsNodeRecord.wrapNodeLabels(sourceLabels);
            case KEY_REL_ID: return GdsRecord.wrapScalar(relId, ValueType.LONG);
            case KEY_REL_TYPE: return GdsRecord.wrapString(relType);
            case KEY_TARGET_ID: return GdsRecord.wrapScalar(target, ValueType.LONG);
            case KEY_TARGET_LABELS: GdsNodeRecord.wrapNodeLabels(targetLabels);
            default:
                throw new RuntimeException("invalid field");
        }
    }

    @Override
    public List<String> keys() {
        return Arrays.stream(keys).collect(Collectors.toList());
    }
}
