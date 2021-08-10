package org.neo4j.arrow.job;

import org.neo4j.arrow.Neo4jRecord;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class GdsRecord implements Neo4jRecord {
    private final Value nodeId;
    private final Value value;

    private GdsRecord(Value value, long nodeId) {
        this.nodeId = wrapScalar(ValueType.LONG, nodeId);
        this.value = value;
    }

    public static GdsRecord wrap(NodeProperties properties, long nodeId) {
        switch (properties.valueType()) {
            case LONG:
                return new GdsRecord(wrapScalar(properties.valueType(), properties.longValue(nodeId)), nodeId);
            case DOUBLE:
                return new GdsRecord(wrapScalar(properties.valueType(), properties.doubleValue(nodeId)), nodeId);
            case LONG_ARRAY:
                return new GdsRecord(wrapLongArray(properties.longArrayValue(nodeId)), nodeId);
            case FLOAT_ARRAY:
                return new GdsRecord(wrapFloatArray(properties.floatArrayValue(nodeId)), nodeId);
            case DOUBLE_ARRAY:
                return new GdsRecord(wrapDoubleArray(properties.doubleArrayValue(nodeId)), nodeId);
            default:
                // XXX tbd string type?
                return new GdsRecord(wrapString(properties.getObject(nodeId).toString()), nodeId);
        }
    }

    protected static Value wrapLongArray(long[] longs) {
        return new Value() {
            final List<Value> values = Arrays.stream(longs)
                    .boxed()
                    .map(val -> wrapScalar(ValueType.LONG, val))
                    .collect(Collectors.toList());

            @Override
            public int asInt() {
                return 0;
            }

            @Override
            public long asLong() {
                return 0;
            }

            @Override
            public float asFloat() {
                return 0;
            }

            @Override
            public double asDouble() {
                return 0;
            }

            @Override
            public String asString() {
                return "";
            }

            @Override
            public List<Value> asList() {
                return values;
            }

            @Override
            public Type type() {
                return Type.LIST;
            }
        };
    }

    protected static Value wrapFloatArray(float[] floats) {
        return new Value() {
            final List<Value> values = LongStream.range(0, floats.length)
                    .mapToObj(idx -> Double.valueOf(floats[(int) idx]))
                    .map(val -> wrapScalar(ValueType.DOUBLE, val))
                    .collect(Collectors.toList());

            @Override
            public int asInt() {
                return 0;
            }

            @Override
            public long asLong() {
                return 0;
            }

            @Override
            public float asFloat() {
                return 0;
            }

            @Override
            public double asDouble() {
                return 0;
            }

            @Override
            public String asString() {
                return "";
            }

            @Override
            public List<Value> asList() {
                return values;
            }

            @Override
            public Type type() {
                return Type.LIST;
            }
        };
    }

    protected static Value wrapDoubleArray(double[] floats) {
        return new Value() {
            final List<Value> values = Arrays.stream(floats)
                    .boxed()
                    .map(val -> wrapScalar(ValueType.DOUBLE, val))
                    .collect(Collectors.toList());

            @Override
            public int asInt() {
                return 0;
            }

            @Override
            public long asLong() {
                return 0;
            }

            @Override
            public float asFloat() {
                return 0;
            }

            @Override
            public double asDouble() {
                return 0;
            }

            @Override
            public String asString() {
                return "";
            }

            @Override
            public List<Value> asList() {
                return values;
            }

            @Override
            public Type type() {
                return Type.LIST;
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
            public List<Value> asList() {
                return List.of();
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
            public List<Value> asList() {
                return List.of();
            }

            @Override
            public Type type() {
                return Type.STRING;
            }
        };
    }

    @Override
    public Value get(int index) {
        if (index == 0)
            return nodeId;
        return value;
    }

    @Override
    public Value get(String field) {
        switch (field) {
            case "nodeId":
                return nodeId;
            case "value":
                return value;
        }
        return null;
    }

    @Override
    public List<String> keys() {
        return List.of("nodeId", "value");
    }
}
