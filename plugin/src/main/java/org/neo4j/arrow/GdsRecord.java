package org.neo4j.arrow;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GdsRecord implements RowBasedRecord {
    private final Value nodeId;

    // TODO: perf optimize by using fixed-width arrays and numeric indexing
    private final Value[] valueArray;
    private final String[] keyArray;

    protected GdsRecord(long nodeId, String[] keys, Value[] values) {
        this.nodeId = wrapScalar(ValueType.LONG, nodeId);
        this.keyArray = keys;
        this.valueArray = values;
    }

    protected GdsRecord(long nodeId, List<String> keys, List<Value> values) {
        this(nodeId, keys.toArray(new String[keys.size()]), values.toArray(new Value[values.size()]));
    }

    public static GdsRecord wrap(long nodeId, String fieldName, NodeProperties properties) {
        switch (properties.valueType()) {
            // TODO: INT?
            case LONG:
                return new GdsRecord(nodeId,
                        new String[] { fieldName },
                        new Value[] { wrapScalar(properties.valueType(), properties.longValue(nodeId)) });
            case DOUBLE:
                return new GdsRecord(nodeId,
                        new String[] { fieldName },
                        new Value[] { wrapScalar(properties.valueType(), properties.doubleValue(nodeId)) });
                // TODO: INT_ARRAY?
            case LONG_ARRAY:
                return new GdsRecord(nodeId, new String[] { fieldName }, new Value[] { (wrapLongArray(properties.longArrayValue(nodeId)))});
            case FLOAT_ARRAY:
                return new GdsRecord(nodeId, new String[] { fieldName }, new Value[] { (wrapFloatArray(properties.floatArrayValue(nodeId)))});
            case DOUBLE_ARRAY:
                return new GdsRecord(nodeId, new String[] { fieldName }, new Value[] { (wrapDoubleArray(properties.doubleArrayValue(nodeId)))});
            default:
                // XXX tbd string type?
                return new GdsRecord(nodeId, new String[] { fieldName }, new Value[] { (wrapString(properties.getObject(nodeId).toString()))});
        }
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
            public double[] asDoubleArray() {
                return null;
            }

            @Override
            public Type type() {
                return Type.LIST;
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
                return IntStream.range(0, floats.length)
                        .mapToObj(idx -> Float.valueOf(floats[idx]))
                        .collect(Collectors.toList());
            }

            @Override
            public List<Integer> asIntList() {
                return IntStream.range(0, floats.length)
                        .mapToObj(idx -> Float.floatToIntBits(floats[idx]))
                        .collect(Collectors.toList());
            }

            @Override
            public List<Long> asLongList() {
                return IntStream.range(0, floats.length)
                        .mapToObj(idx -> Double.doubleToLongBits(floats[idx]))
                        .collect(Collectors.toList());
            }

            @Override
            public List<Float> asFloatList() {
                return IntStream.range(0, floats.length)
                        .mapToObj(idx -> Float.valueOf(floats[idx]))
                        .collect(Collectors.toList());
            }

            @Override
            public float[] asFloatArray() {
                return floats;
            }

            @Override
            public List<Double> asDoubleList() {
                return IntStream.range(0, floats.length)
                        .mapToObj(idx -> Double.valueOf(floats[idx]))
                        .collect(Collectors.toList());
            }

            @Override
            public double[] asDoubleArray() {
                return null;
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
                return null; //values.stream().map(v -> (Object)v).collect(Collectors.toList());
            }

            @Override
            public List<Integer> asIntList() {
                return null; //values.stream().mapToInt(Double::intValue).boxed().collect(Collectors.toList());
            }

            @Override
            public List<Long> asLongList() {
                return null; // values.stream().mapToLong(Double::longValue).boxed().collect(Collectors.toList());
            }

            @Override
            public List<Float> asFloatList() {
                return null; // values.stream().map(Double::floatValue).collect(Collectors.toList());
            }

            @Override
            public List<Double> asDoubleList() {
                ArrayList<Double> list = new ArrayList<>(doubles.length);
                for (double d : doubles) {
                    list.add(d);
                }
                return list;
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
            public int size() {
                return 1;
            }

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
            public List<Object> asList() {
                return List.of();
            }

            @Override
            public List<Integer> asIntList() {
                return null;
            }

            @Override
            public List<Long> asLongList() {
                return null;
            }

            @Override
            public List<Float> asFloatList() {
                return null;
            }

            @Override
            public List<Double> asDoubleList() {
                return null;
            }

            @Override
            public double[] asDoubleArray() {
                return null;
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
            private final String str = s;

            @Override
            public int size() {
                return 1;
            }

            @Override
            public int asInt() {
                return 0;
            }

            @Override
            public long asLong() {
                return 0L;
            }

            @Override
            public float asFloat() {
                return 0f;
            }

            @Override
            public double asDouble() {
                return 0d;
            }

            @Override
            public String asString() {
                return s;
            }

            @Override
            public List<Object> asList() {
                return List.of();
            }

            @Override
            public List<Integer> asIntList() {
                return null;
            }

            @Override
            public List<Long> asLongList() {
                return null;
            }

            @Override
            public List<Float> asFloatList() {
                return null;
            }

            @Override
            public List<Double> asDoubleList() {
                return null;
            }

            @Override
            public double[] asDoubleArray() {
                return null;
            }

            @Override
            public Type type() {
                return Type.STRING;
            }
        };
    }

    @Override
    public Value get(int index) {
        assert index > 0 && index < (valueArray.length - 1);
        if (index == 0)
            return nodeId;
        return valueArray[index - 1];
    }

    @Override
    public Value get(String field) {
        switch (field) {
            case "nodeId":
                return nodeId;
            default:
                for (int i=0; i<keyArray.length; i++)
                    if (keyArray[i].equals(field))
                        return valueArray[i];
        }
        return null;
    }

    @Override
    public List<String> keys() {
        ArrayList<String> list = new ArrayList<>(keyArray.length + 1);
        list.add("nodeId");
        list.addAll(List.of(keyArray));
        return list;
    }
}
