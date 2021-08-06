package org.neo4j.arrow;

import org.neo4j.driver.Record;

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
            public List<Value> asList() {
                return value.asList(Neo4jDriverRecord::wrapValue);
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
