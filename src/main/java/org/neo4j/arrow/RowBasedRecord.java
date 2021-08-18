package org.neo4j.arrow;

import java.util.List;

/**
 * Adapter for different ways to represent raw values from Neo4j. For instance, from a Driver or
 * the underlying Server tx api.
 */
public interface RowBasedRecord {

    /**
     * Supported record types. Each {@link Type} should have a logical mapping to an Arrow type.
     */
    enum Type {
        /** Scalar integer (32 bit, signed) */
        INT,
        /** Array of integers (32 bit, signed) */
        INT_ARRAY,
        /** Scalar long (64 bit, signed) */
        LONG,
        /** Array of longs (64 bit, signed) */
        LONG_ARRAY,
        /** Scalar floating point (single precision, 32-bit) */
        FLOAT,
        /** Array of floating points (single precision, 32-bit) */
        FLOAT_ARRAY,
        /** Scalar of floating point (double precision, 64-bit) */
        DOUBLE,
        /** Array of floating points (double precision, 64-bit) */
        DOUBLE_ARRAY,
        /** Variable length UTF-8 string (varchar) */
        STRING,
        /** Heterogeneous array of supported {@link Type}s */
        LIST,
        /** Catch-all...TBD */
        OBJECT
    }

    /** Retrieve a {@link Value} from the record by positional index */
    Value get(int index);
    /** Retrieve a {@link Value} from the record by named field */
    Value get(String field);

    /** List of fields in this {@link RowBasedRecord} */
    List<String> keys();

    /**
     *
     */
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
