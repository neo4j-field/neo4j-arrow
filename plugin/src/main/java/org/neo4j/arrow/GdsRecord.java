package org.neo4j.arrow;

import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class GdsRecord implements RowBasedRecord {

    protected final Value[] valueArray;
    protected final String[] keyArray;

    protected GdsRecord(String[] keyArray, Value[] valueArray) {
        this.keyArray = keyArray;
        this.valueArray = valueArray;
    }

    protected static Value wrapLongArray(long[] longs) {
        return new Value() {
            final List<Long> values = Arrays.stream(longs).boxed().collect(Collectors.toList());

            @Override
            public int size() {
                return longs.length;
            }

            @Override
            public List<Object> asList() {
                return Arrays.asList(values.toArray());
            }

            @Override
            public List<Integer> asIntList() {
                return values.stream().mapToInt(Long::intValue).boxed().collect(Collectors.toList());
            }

            @Override
            public List<Long> asLongList() {
                return values;
            }

            @Override
            public List<Float> asFloatList() {
                return values.stream().map(Long::floatValue).collect(Collectors.toList());
            }

            @Override
            public List<Double> asDoubleList() {
                return values.stream().mapToDouble(Long::doubleValue).boxed().collect(Collectors.toList());
            }

            @Override
            public Type type() {
                return Type.LONG_ARRAY;
            }
        };
    }

    protected static Value wrapIntArray(int[] ints) {
        return new Value() {
            @Override
            public int size() {
                return ints.length;
            }

            @Override
            public List<Object> asList() {
                return IntStream.range(0, ints.length)
                        .boxed()
                        .collect(Collectors.toList());
            }

            @Override
            public List<Integer> asIntList() {
                return IntStream.range(0, ints.length)
                        .boxed()
                        .collect(Collectors.toList());
            }

            @Override
            public List<Long> asLongList() {
                return IntStream.range(0, ints.length)
                        .mapToLong(i -> (long)i)
                        .boxed()
                        .collect(Collectors.toList());
            }

            @Override
            public int[] asIntArray() {
                return ints;
            }

            @Override
            public Type type() {
                return Type.INT_ARRAY;
            }
        };
    }

    protected static Value wrapFloatArray(float[] floats) {
        return new Value() {

            @Override
            public int size() {
                return floats.length;
            }

            @Override
            public List<Object> asList() {
                return List.of(floats);
            }

            @Override
            public List<Float> asFloatList() {
                final ArrayList<Float> list = new ArrayList<>();
                for (float f : floats)
                    list.add(f);
                return list;
            }

            @Override
            public List<Double> asDoubleList() {
                return asFloatList().stream()
                        .mapToDouble(Float::doubleValue).boxed().collect(Collectors.toList());
            }

            @Override
            public double[] asDoubleArray() {
                final double[] data = new double[floats.length];
                for (int i=0; i<data.length; i++)
                    data[i] = floats[i];
                return data;
            }

            @Override
            public Type type() {
                return Type.FLOAT_ARRAY;
            }
        };
    }


    protected static Value wrapDoubleArray(double[] doubles) {
        return new Value() {

            @Override
            public int size() {
                return doubles.length;
            }

            @Override
            public List<Object> asList() {
                return Arrays.stream(doubles).boxed().collect(Collectors.toList());
            }

            @Override
            public List<Integer> asIntList() {
                return Arrays.stream(doubles).boxed().mapToInt(Double::intValue).boxed().collect(Collectors.toList());
            }

            @Override
            public List<Long> asLongList() {
                return Arrays.stream(doubles).boxed().mapToLong(Double::longValue).boxed().collect(Collectors.toList());
            }

            @Override
            public List<Float> asFloatList() {
                return Arrays.stream(doubles).mapToObj(d -> (float) d).collect(Collectors.toList());
            }

            @Override
            public List<Double> asDoubleList() {
                return Arrays.stream(doubles).boxed().collect(Collectors.toList());
            }

            @Override
            public double[] asDoubleArray() {
                return doubles;
            }

            @Override
            public Type type() {
                return Type.DOUBLE_ARRAY;
            }
        };
    }

    protected static Value wrapScalar(ValueType t, Number n) {
        return new Value() {
            private final Number num = n;
            private final ValueType valueType = t;

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
                return num.toString();
            }

            @Override
            public Type type() {
                switch (valueType) {
                    case LONG:
                        return Type.LONG;
                    case DOUBLE:
                        return Type.DOUBLE;
                    case FLOAT_ARRAY:
                    case LONG_ARRAY:
                    case DOUBLE_ARRAY:
                        return Type.LIST;
                    default:
                        return Type.OBJECT;
                }
            }
        };
    }

    protected static Value wrapString(String s) {
        return new Value() {
            @Override
            public String asString() {
                return s;
            }

            @Override
            public Type type() {
                return Type.STRING;
            }
        };
    }

}
