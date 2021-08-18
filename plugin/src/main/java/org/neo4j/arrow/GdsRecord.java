package org.neo4j.arrow;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Wrapper around a record from the in-memory graph in GDS.
 * <p>
 *     GDS gives us the ability to use fixed-width data structures, specifically certain Java
 *     primitives. This dramatically increases the efficiency and performance of Apache Arrow. As
 *     such, we use as many Java arrays as we can in lieu of boxed types in {@link List}s.
 * </p>
 * <p><em>TODO: Currently this assumes only Node properties. Needs support for Rels!</em></p>
 */
public class GdsRecord implements RowBasedRecord {
    /** Represents the underlying node id */
    private final Value nodeId;

    private final Value[] valueArray;
    private final String[] keyArray;

    protected GdsRecord(long nodeId, String[] keys, Value[] values) {
        this.nodeId = wrapScalar(ValueType.LONG, nodeId);
        this.keyArray = keys;
        this.valueArray = values;
    }

    protected GdsRecord(long nodeId, List<String> keys, List<Value> values) {
        this(nodeId, keys.toArray(new String[0]), values.toArray(new Value[0]));
    }

    /**
     * Wrap the given GDS information into a single {@link GdsRecord}.
     *
     * @param nodeId the native node id of the record
     * @param fieldName the name of the property or field
     * @param properties a reference to the {@link NodeProperties} interface for resolving the value
     * @return a new {@link GdsRecord}
     */
    public static GdsRecord wrap(long nodeId, String fieldName, NodeProperties properties) {
        return wrap(nodeId, new String[] { fieldName }, new NodeProperties[] { properties });
    }

    public static GdsRecord wrap(long nodeId, List<String> fieldNames, List<NodeProperties> propertiesList) {
        return wrap(nodeId, fieldNames.toArray(new String[0]),
                propertiesList.toArray(new NodeProperties[0]));
    }

    public static GdsRecord wrap(long nodeId, String[] fieldNames, NodeProperties[] propertiesArray) {
        final Value[] values = new Value[propertiesArray.length];

        assert fieldNames.length == values.length;

        for (int i=0; i<fieldNames.length; i++) {
            final NodeProperties properties = propertiesArray[i];
            Value value;
            switch (properties.valueType()) {
                // TODO: INT? Does it exist?
                case LONG:
                    value = wrapScalar(properties.valueType(), properties.longValue(nodeId));
                    break;
                case DOUBLE:
                    value = wrapScalar(properties.valueType(), properties.doubleValue(nodeId));
                    break;
                // TODO: INT_ARRAY?
                case LONG_ARRAY:
                    value = wrapLongArray(properties.longArrayValue(nodeId));
                    break;
                case FLOAT_ARRAY:
                    value = wrapFloatArray(properties.floatArrayValue(nodeId));
                    break;
                case DOUBLE_ARRAY:
                    value = wrapDoubleArray(properties.doubleArrayValue(nodeId));
                    break;
                default:
                    // TODO: String? Object? What should we do?
                    value = wrapString(Objects.requireNonNull(properties.getObject(nodeId)).toString());
            }
            values[i] = value;
        }
        return new GdsRecord(nodeId, fieldNames, values);
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
                        .mapToObj(idx -> floats[idx])
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
                        .mapToObj(idx -> floats[idx])
                        .collect(Collectors.toList());
            }

            @Override
            public float[] asFloatArray() {
                return floats;
            }

            @Override
            public List<Double> asDoubleList() {
                return IntStream.range(0, floats.length)
                        .mapToObj(idx -> (double) floats[idx])
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
                return Arrays.stream(doubles).mapToObj(d -> (float)d).collect(Collectors.toList());
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

    @Override
    public Value get(int index) {
        assert (0 <= index && index <= valueArray.length);
        if (index == 0)
            return nodeId;
        return valueArray[index - 1];
    }

    @Override
    public Value get(String field) {
        if ("nodeId".equals(field)) {
            return nodeId;
        } else {
            for (int i = 0; i < keyArray.length; i++)
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
