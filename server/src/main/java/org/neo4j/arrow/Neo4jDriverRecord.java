package org.neo4j.arrow;

import org.neo4j.driver.Record;
import org.neo4j.driver.internal.types.InternalTypeSystem;

import java.util.List;

public class Neo4jDriverRecord implements Neo4jRecord {
    private final Record record;

    private Neo4jDriverRecord(Record record) {
        this.record = record;
    }

    public static Neo4jRecord wrap(Record record) {
        return new Neo4jDriverRecord(record);
    }

    private static Neo4jRecord.Value wrapValue(org.neo4j.driver.Value value) {
        return new Value() {
            @Override
            public int size() {
                if (value.hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
                    return value.asList().size();
                return 1;
            }

            @Override
            public int asInt() {
                return value.asInt();
            }

            @Override
            public long asLong() {
                return value.asLong();
            }

            @Override
            public float asFloat() {
                return value.asFloat();
            }

            @Override
            public double asDouble() {
                return value.asDouble();
            }

            @Override
            public String asString() {
                return value.asString();
            }

            @Override
            public List<Object> asList() {
                return value.asList(Neo4jDriverRecord::wrapValue);
            }

            @Override
            public List<Integer> asIntList() {
                return List.of();
            }

            @Override
            public List<Long> asLongList() {
                return List.of();
            }

            @Override
            public List<Float> asFloatList() {
                return List.of();
            }

            @Override
            public List<Double> asDoubleList() {
                return List.of();
            }

            public Type type() {
                switch (value.type().name()) {
                    case "INTEGER":
                        return Type.INT;
                    case "LONG":
                        return Type.LONG;
                    case "FLOAT":
                    case "DOUBLE":
                        return Type.DOUBLE;
                    case "STRING":
                        return Type.STRING;
                    case "LIST OF ANY?":
                        return Type.LIST;
                }
                // Catch-all
                return Type.OBJECT;
            }
        };
    }

    @Override
    public Value get(int index) {
        return wrapValue(record.get(index));
    }

    @Override
    public Value get(String field) {
        return wrapValue(record.get(field));
    }

    @Override
    public List<String> keys() {
        return record.keys();
    }
}
