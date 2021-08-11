package org.neo4j.arrow;

import java.util.List;

/**
 * Adapter for different ways to represent raw values from Neo4j. For instance, from a Driver or
 * the underlying Server tx api.
 */
public interface RowBasedRecord {

    enum Type {
        INT,
        INT_ARRAY,
        LONG,
        LONG_ARRAY,
        FLOAT,
        FLOAT_ARRAY,
        DOUBLE,
        DOUBLE_ARRAY,
        STRING,
        LIST,
        OBJECT
    }

    Value get(int index);
    Value get(String field);

    List<String> keys();

    interface Value {
        /* Number of primitives or inner values */
        int size();

        int asInt();

        long asLong();

        float asFloat();

        double asDouble();

        String asString();

        List<Object> asList();

        List<Integer> asIntList();

        List<Long> asLongList();

        List<Float> asFloatList();

        List<Double> asDoubleList();

        Type type();
    }
}
