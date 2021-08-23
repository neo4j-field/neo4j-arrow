package org.neo4j.arrow;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.List;

public class GdsRelationshipRecord extends GdsRecord {

    private static final String SOURCE_FIELD = "sourceId";
    private static final String TARGET_FIELD = "targetId";

    private final Value sourceId;
    private final Value targetId;

    public GdsRelationshipRecord(long sourceId, long targetId, String[] keyArray, Value[] valueArray) {
        super(keyArray, valueArray);
        this.sourceId = wrapScalar(ValueType.LONG, sourceId);
        this.targetId = wrapScalar(ValueType.LONG, targetId);
    }

    @Override
    public Value get(int index) {
        if (index < 0 || index > valueArray.length + 1)
            throw new RuntimeException("invalid index");

        switch (index) {
            case 0:
                return sourceId;
            case 1:
                return targetId;
            default:
                return valueArray[index-2];
        }
    }

    @Override
    public Value get(String field) {
        if (field.equalsIgnoreCase(SOURCE_FIELD))
            return sourceId;
        if (field.equalsIgnoreCase(TARGET_FIELD))
            return targetId;

        for (int i = 0; i < keyArray.length; i++)
            if (keyArray[i].equals(field)) {
                return valueArray[i];
            }

        throw new RuntimeException("invalid field");
    }

    @Override
    public List<String> keys() {
        ArrayList<String> list = new ArrayList<>(keyArray.length + 1);
        list.add(SOURCE_FIELD);
        list.add(TARGET_FIELD);
        list.addAll(List.of(keyArray));
        return list;    }
}
