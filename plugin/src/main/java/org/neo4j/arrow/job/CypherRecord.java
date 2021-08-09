package org.neo4j.arrow.job;

import org.neo4j.arrow.Neo4jRecord;
import org.neo4j.graphdb.Result;

import java.util.*;
import java.util.stream.Collectors;

public class CypherRecord implements Neo4jRecord {

    private final Map<String, Value> map;
    private final ArrayList<String> keys;

    protected CypherRecord(Map<String, Object> m) {
        this.map = new HashMap<>();
        m.forEach((s, o) -> this.map.put(s, wrapObject(o)));
        this.keys = new ArrayList<>();
        this.keys.addAll(this.map.keySet());
    }

    protected static CypherRecord wrap(Map<String, Object> map) {
        return new CypherRecord(map);
    }

    protected static CypherRecord wrap(Result.ResultRow row, Collection<String> columns) {
        final Map<String, Object> map = new HashMap<>();
        for (String column : columns) {
            map.put(column, row.get(column));
        }
        return new CypherRecord(map);
    }

    protected static Value wrapObject(Object o) {
        // Best effort translation.
        return new Value() {
            private final Object obj = o;
            @Override
            public int asInt() {
                if (obj instanceof Integer)
                    return (int) obj;
                else if (obj instanceof Number)
                    return ((Number) obj).intValue();
                return Integer.parseInt(obj.toString());
            }

            @Override
            public long asLong() {
                if (obj instanceof Long)
                    return (long) obj;
                else if (obj instanceof Number)
                    return ((Number) obj).longValue();
                return Long.parseLong(obj.toString());
            }

            @Override
            public float asFloat() {
                if (obj instanceof Float)
                    return (float) obj;
                else if (obj instanceof Number)
                    return ((Number) obj).floatValue();
                return Float.parseFloat(obj.toString());
            }

            @Override
            public double asDouble() {
                if (obj instanceof Double)
                    return (double) obj;
                else if (obj instanceof Number)
                    return ((Number) obj).doubleValue();
                return Double.parseDouble(obj.toString());
            }

            @Override
            public String asString() {
                return obj.toString();
            }

            @Override
            public List<Value> asList() {
                if (obj instanceof List<?>) {
                    return ((List<?>)obj).stream()
                            .map(CypherRecord::wrapObject)
                            .collect(Collectors.toList());
                }
                return List.of();
            }

            @Override
            public Type type() {
                if (obj instanceof Integer)
                    return Type.INT;
                if (obj instanceof Long)
                    return Type.LONG;
                if (obj instanceof Float)
                    return Type.FLOAT;
                if (obj instanceof Double)
                    return Type.DOUBLE;
                if (obj instanceof String)
                    return Type.STRING;
                if (obj instanceof List)
                    return Type.LIST;
                return Type.OBJECT;
            }
        };
    }

    @Override
    public Value get(int index) {
        return map.get(keys.get(index));
    }

    @Override
    public Value get(String field) {
        return map.get(field);
    }

    @Override
    public List<String> keys() {
        return keys;
    }
}
