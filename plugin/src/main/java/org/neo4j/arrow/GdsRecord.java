package org.neo4j.arrow;

import java.util.List;

public class GdsRecord implements Neo4jRecord {
    private final Value value;

    private GdsRecord(Object o) {
        this.value = wrapObject((double) o);
    }

    public static GdsRecord wrap(Object o) {
        return new GdsRecord(o);
    }

    public static Value wrapObject(Double d) {
        return new Value() {
            @Override
            public int asInt() {
                return d.intValue();
            }

            @Override
            public long asLong() {
                return d.longValue();
            }

            @Override
            public float asFloat() {
                return d.floatValue();
            }

            @Override
            public double asDouble() {
                return d;
            }

            @Override
            public String asString() {
                return String.valueOf(d);
            }

            @Override
            public List<Value> asList() {
                return List.of();
            }

            @Override
            public Type type() {
                return Type.DOUBLE;
            }
        };
    }

    @Override
    public Value get(int index) {
        return value;
    }

    @Override
    public Value get(String field) {
        return value;
    }

    @Override
    public List<String> keys() {
        return List.of("n");
    }
}
