package org.neo4j.arrow.job;

import org.neo4j.arrow.Neo4jRecord;

import java.util.List;

public class GdsRecord implements Neo4jRecord {
    private final Value nodeId;
    private final Value value;

    private GdsRecord(long nodeId, Number num, Type type) {
        this.nodeId = wrapObject(nodeId, Type.LONG);
        this.value = wrapObject(num, type);
    }

    public static GdsRecord wrap(long nodeId, Object o) {
        // XXX assume only Number's for now since it's GDS and assume a simple nodeId + value pair
        Number num = (Number) o;
        if (num instanceof Float)
            return new GdsRecord(nodeId, num, Type.FLOAT);
        else if (num instanceof Double)
            return new GdsRecord(nodeId, num, Type.DOUBLE);
        return new GdsRecord(nodeId, num, Type.LONG);
    }

    protected static Value wrapObject(Number num, Type type) {
        return new Value() {
            @Override
            public int asInt() {
                return num.intValue();
            }

            @Override
            public long asLong() {
                return num.longValue();
            }

            @Override
            public float asFloat() {
                return num.floatValue();
            }

            @Override
            public double asDouble() {
                return num.doubleValue();
            }

            @Override
            public String asString() {
                return String.valueOf(num);
            }

            @Override
            public List<Value> asList() {
                return List.of();
            }

            @Override
            public Type type() {
                return type;
            }
        };
    }

    @Override
    public Value get(int index) {
        if (index == 0)
            return nodeId;
        return value;
    }

    @Override
    public Value get(String field) {
        switch (field) {
            case "nodeId":
                return nodeId;
            case "value":
                return value;
        }
        return null;
    }

    @Override
    public List<String> keys() {
        return List.of("nodeId", "value");
    }
}
