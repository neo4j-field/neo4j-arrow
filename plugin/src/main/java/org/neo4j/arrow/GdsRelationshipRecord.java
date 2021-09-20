package org.neo4j.arrow;

import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.List;

/**
 *  A simplified record that mimics the structure output by gds.graph.streamRelationships()
 */
public class GdsRelationshipRecord extends GdsRecord {

    public static final String SOURCE_FIELD = "sourceId";
    public static final String TARGET_FIELD = "targetId";
    public static final String TYPE_FIELD = "relType";
    public static final String PROPERTY_FIELD = "property";
    public static final String VALUE_FIELD = "value";

    private final Value sourceId;
    private final Value targetId;
    private final Value relType;
    private final Value property;
    private final Value value;

    public GdsRelationshipRecord(long sourceId, long targetId, String type, String property, Value value) {
        super(new String[0], new Value[0]);
        this.sourceId = wrapScalar(ValueType.LONG, sourceId);
        this.targetId = wrapScalar(ValueType.LONG, targetId);
        this.relType = wrapString(type);
        this.property = wrapString(property);
        this.value = value;
    }

    @Override
    public Value get(int index) {
        switch (index) {
            case 0:
                return sourceId;
            case 1:
                return targetId;
            case 2:
                return relType;
            case 3:
                return property;
            case 4:
                return value;
            default:
                throw new RuntimeException("invalid index");
        }
    }

    @Override
    public Value get(String field) {
        switch (field) {
            case SOURCE_FIELD:
                return sourceId;
            case TARGET_FIELD:
                return targetId;
            case TYPE_FIELD:
                return relType;
            case PROPERTY_FIELD:
                return property;
            case VALUE_FIELD:
                return value;
            default:
                throw new RuntimeException("invalid field");
        }
    }

    @Override
    public List<String> keys() {
        return List.of(SOURCE_FIELD, TARGET_FIELD, TYPE_FIELD, PROPERTY_FIELD, VALUE_FIELD);
    }
}
