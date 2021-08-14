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
        default int size() {
            return 1;
        }

        default int asInt() {
            return 0;
        }

        default long asLong() {
            return 0L;
        }

        default float asFloat() {
            return 0f;
        }

        default double asDouble() {
            return 0d;
        }

        default String asString() {
            return "";
        }

        default List<Object> asList() {
            return List.of();
        }

        default List<Integer> asIntList() {
            return List.of();
        }

        default int[] asIntArray() {
            return new int[0];
        }

        default List<Long> asLongList() {
            return List.of();
        }

        default long[] asLongArray() {
            return new long[0];
        }

        default List<Float> asFloatList() {
            return List.of();
        }

        default float[] asFloatArray() {
            return new float[0];
        }

        default List<Double> asDoubleList() {
            return List.of();
        }

        default double[] asDoubleArray() {
            return new double[0];
        }

        default Type type() {
            return Type.OBJECT;
        }
    }
}
