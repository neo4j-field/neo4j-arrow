package org.neo4j.arrow;

import org.neo4j.graphdb.Result;

import java.util.*;
import java.util.stream.Collectors;

public class ServerGenericRecord implements Neo4jRecord {

    private final Map<String, Value> map;
    private final ArrayList<String> keys;

    protected ServerGenericRecord(Map<String, Object> map) {

        this.map = new HashMap<>();
        map.forEach((s, o) -> map.put(s, wrapObject(o)));
        this.keys = new ArrayList<>();
        this.keys.addAll(map.keySet());
    }

    protected static Neo4jRecord wrap(Map<String, Object> map) {
        return new ServerGenericRecord(map);
    }

    protected static Neo4jRecord wrap(Result.ResultRow row, Collection<String> columns) {
        final Map<String, Object> map = new HashMap<>();
        for (String column : columns) {
            map.put(column, row.get(column));
        }
        return wrap(map);
    }

    protected static Value wrapObject(Object obj) {
        // Best effort translation.
        return new Value() {
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
                            .map(ServerGenericRecord::wrapObject)
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
        return wrapObject(map.get(keys.get(index)));
    }

    @Override
    public Value get(String field) {
        return wrapObject(map.get(field));
    }

    @Override
    public List<String> keys() {
        return keys;
    }
}
