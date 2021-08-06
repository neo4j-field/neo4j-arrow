package org.neo4j.arrow;

import java.util.List;

/**
 * Adapter for different ways to represent raw values from Neo4j. For instance, from a Driver or
 * the underlying Server tx api.
 */
public interface Neo4jRecord {

    enum Type {
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        LIST,
        OBJECT
    }

    Value get(int index);
    Value get(String field);

    List<String> keys();

    interface Value {
        int asInt();

        long asLong();

        float asFloat();

        double asDouble();

        String asString();

        List<Value> asList();

        Type type();
    }
}
