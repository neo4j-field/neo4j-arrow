package org.neo4j.arrow;

import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class GdsRelRecord extends GdsRecord {
    private final Value sourceId;
    private final Value targetId;
    private final Value type;

    protected GdsRelRecord(long sourceId, long targetId, String type,
                           String[] keyArray, Value[] valueArray) {
        super(keyArray, valueArray);
        this.sourceId = wrapScalar(ValueType.LONG, sourceId);
        this.targetId = wrapScalar(ValueType.LONG, targetId);
        this.type = wrapString(type);
    }

    public static GdsRelRecord wrap(RelationshipCursor cursor, String type,
                                    )

    @Override
    public Value get(int index) {
        switch (index) {
            case 0:
                return sourceId;
            case 1:
                return targetId;
            case 2:
                return type;
            default:
                if (2 < index && index < valueArray.length)
                    return valueArray[index - 3];
        }
        throw new RuntimeException("invalid index");
    }

    @Override
    public Value get(String field) {
        switch (field) {
            case "sourceId":
                return sourceId;
            case "targetId":
                return targetId;
            case "type":
                return type;
            default:
                for (int i = 0; i < keyArray.length; i++)
                    if (keyArray[i].equals(field))
                        return valueArray[i];
        }
        throw new RuntimeException("invalid field");
    }

    @Override
    public List<String> keys() {
        ArrayList<String> list = new ArrayList<>();
        list.add("sourceId");
        list.add("targetId");
        list.add("type");
        list.addAll(List.of(keyArray));
        return list;
    }
}
