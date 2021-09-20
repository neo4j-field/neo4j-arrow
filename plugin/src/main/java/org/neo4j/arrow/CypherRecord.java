package org.neo4j.arrow;

import org.neo4j.graphdb.Result;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.*;
import org.neo4j.values.virtual.ListValue;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A wrapper around the raw Record returned via the Transaction API.
 * <p>
 * Since the Transaction API basically gives us opaque objects, we need to do lots of type checking
 * and casting. (This is frustrating.)
 */
public class CypherRecord implements RowBasedRecord {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CypherRecord.class);

    protected final Value[] valueArray;
    protected final String[] keyArray;

    protected CypherRecord(String[] keyArray, Value[] valueArray) {
        this.keyArray = keyArray;
        this.valueArray = valueArray;
    }

    protected CypherRecord(Map<String, Object> m) {
        keyArray = m.keySet().toArray(new String[0]);
        valueArray = new Value[keyArray.length];

        for (int i=0; i<keyArray.length; i++) {
            valueArray[i] = wrapObject(m.get(keyArray[i]));
        }
    }

    public static CypherRecord wrap(String[] keyArray, AnyValue[] anyValues) throws Exception {
        try {
            return new CypherRecord(
                    keyArray,
                    Arrays.stream(anyValues)
                            .map(CypherRecord::wrapAnyValue)
                            .toArray(Value[]::new));
        } catch (RuntimeException e) {
            // TODO: new Exception class
            throw new Exception(e);
        }
    }

    public static CypherRecord wrap(Map<String, Object> map) {
        return new CypherRecord(map);
    }

    public static CypherRecord wrap(Result.ResultRow row, Collection<String> columns) {
        final Map<String, Object> map = new HashMap<>();
        for (String column : columns) {
            map.put(column, row.get(column));
        }
        return new CypherRecord(map);
    }

    protected static Value wrapNumberValue(NumberValue numberValue) {
        return new Value() {
            @Override
            public int asInt() {
                return (int) numberValue.longValue();
            }

            @Override
            public long asLong() {
                return numberValue.longValue();
            }

            @Override
            public float asFloat() {
                return (float) numberValue.doubleValue();
            }

            @Override
            public double asDouble() {
                return numberValue.doubleValue();
            }

            @Override
            public String asString() {
                return numberValue.toString();
            }

            @Override
            public Type type() {
                switch (numberValue.numberType()) {
                    case INTEGRAL:
                        return Type.LONG;
                    default:
                        return Type.DOUBLE;
                }
            }
        };
    }

    protected static Value wrapIntegralList(ListValue listValue) {
        return new Value() {
            @Override
            public int size() {
                return listValue.length();
            }

            @Override
            public List<Object> asList() {
                return Arrays.stream(listValue.asArray()).collect(Collectors.toList());
            }

            @Override
            public List<Long> asLongList() {
                return Arrays.stream(listValue.asArray())
                        .mapToLong(val -> ((NumberValue)val).longValue())
                        .boxed()
                        .collect(Collectors.toList());
            }

            @Override
            public long[] asLongArray() {
                return Arrays.stream(listValue.asArray())
                        .mapToLong(val -> ((NumberValue)val).longValue())
                        .toArray();
            }

            @Override
            public Type type() {
                return Type.LONG_ARRAY;
            }
        };
    }

    protected static Value wrapFloatList(ListValue listValue) {
        return new Value() {
            @Override
            public int size() {
                return listValue.length();
            }

            @Override
            public List<Float> asFloatList() {
                return Arrays.stream(listValue.asArray())
                        .mapToDouble(val -> ((NumberValue)val).doubleValue())
                        .boxed()
                        .map(Double::floatValue)
                        .collect(Collectors.toList());
            }

            @Override
            public float[] asFloatArray() {
                // TODO: clean this Double to Float mess :-(
                final float[] data = new float[listValue.length()];
                final AnyValue[] anyValues = listValue.asArray();
                for (int i=0; i<data.length; i++)
                    data[i] = (float) ((NumberValue)anyValues[i]).doubleValue();
                return data;
            }

            @Override
            public List<Double> asDoubleList() {
                return Arrays.stream(listValue.asArray())
                        .mapToDouble(val -> ((NumberValue)val).doubleValue())
                        .boxed()
                        .collect(Collectors.toList());
            }

            @Override
            public double[] asDoubleArray() {
                return Arrays.stream(listValue.asArray())
                        .mapToDouble(val -> ((NumberValue)val).doubleValue())
                        .toArray();
            }

            @Override
            public Type type() {
                return Type.FLOAT_ARRAY;
            }
        };
    }

    protected static Value wrapDoubleList(ListValue listValue) {
        return new Value() {
            @Override
            public int size() {
                return listValue.length();
            }

            @Override
            public List<Double> asDoubleList() {
                return Arrays.stream(listValue.asArray())
                        .mapToDouble(val -> ((NumberValue)val).doubleValue())
                        .boxed()
                        .collect(Collectors.toList());
            }

            @Override
            public double[] asDoubleArray() {
                return Arrays.stream(listValue.asArray())
                        .mapToDouble(val -> ((NumberValue)val).doubleValue())
                        .toArray();
            }

            @Override
            public Type type() {
                return Type.DOUBLE_ARRAY;
            }
        };
    }

    protected static Value wrapListValue(ListValue listValue) {
        if (listValue.nonEmpty()) {
            switch (listValue.getTypeName()) {
                case "IntegerArray":
                case "LongArray":
                case "ShortArray":
                    // XXX treat all as "IntegralList" for now
                    return wrapIntegralList(listValue);
                case "FloatArray":
                    return wrapFloatList(listValue);
                case "List":
                    // XXX treat as a "DoubleArray" for now
                    // Fallthrough
                case "DoubleArray":
                    return wrapDoubleList(listValue);
                default:
                    logger.error("only handles numeric lists :-( ({})", listValue.getTypeName());
                    throw new RuntimeException("only handles numeric lists :-(");
            }
        } else {
            logger.error("can't handle empty lists yet :-(");
            throw new RuntimeException("can't handle empty lists yet :-(");
        }
    }

    protected static Value wrapArrayValue(ArrayValue arrayValue) {
        // TODO: object copy might be slow, check on this
        if (arrayValue instanceof FloatArray) {
            return GdsRecord.wrapFloatArray(((FloatArray) arrayValue).asObjectCopy());
        } else if (arrayValue instanceof DoubleArray) {
            return GdsRecord.wrapDoubleArray(((DoubleArray) arrayValue).asObjectCopy());
        } else if (arrayValue instanceof IntArray) {
            return GdsRecord.wrapIntArray(((IntArray) arrayValue).asObjectCopy());
        } else if (arrayValue instanceof LongArray) {
            return GdsRecord.wrapLongArray(((LongArray) arrayValue).asObjectCopy());
        } // TODO: ShortArray
        logger.error("unhandled ArrayValue of type {}", arrayValue.getTypeName());
        return null;
    }

    protected static Value wrapStringValue(StringValue stringValue) {
        return new Value() {
            @Override
            public String asString() {
                return stringValue.stringValue();
            }

            @Override
            public Type type() {
                return Type.STRING;
            }
        };
    }

    protected static Value wrapAnyValue(AnyValue anyValue) {
        try {
            if (anyValue instanceof NumberValue) {
                return wrapNumberValue((NumberValue) anyValue);
            } else if (anyValue instanceof StringValue) {
                return wrapStringValue((StringValue) anyValue);
            } else if (anyValue instanceof ListValue) {
                return wrapListValue((ListValue) anyValue);
            } else if (anyValue instanceof ArrayValue) {
                return wrapArrayValue((ArrayValue) anyValue);
            }
            logger.error("crap...can't handle type {}", anyValue.getTypeName());
        } catch (Exception e) {
            logger.error("bad juju", e);
            throw new RuntimeException(e);
        }
        throw new RuntimeException("only handles Numbers or List values :-(");
    }

    protected static Value wrapObject(Object o) {
        // Best effort translation.
        return new Value() {
            private final Object obj = o;

            @Override
            public int size() {
                if (obj instanceof List)
                    return ((List<?>)obj).size();
                return 1;
            }

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
            public List<Object> asList() {
                if (obj instanceof List<?>) {
                    return new ArrayList<>(((List<?>) obj));
                }
                return List.of();
            }

            @Override
            public List<Integer> asIntList() {
                if (obj instanceof List) {
                    List<?> list = (List<?>)obj;
                    return list.stream()
                            .mapToInt(o -> wrapObject(o).asInt())
                            .boxed()
                            .collect(Collectors.toList());
                }
                return List.of();
            }

            @Override
            public List<Long> asLongList() {
                if (obj instanceof List) {
                    List<?> list = (List<?>)obj;
                    return list.stream()
                            .mapToLong(o -> wrapObject(o).asLong())
                            .boxed()
                            .collect(Collectors.toList());
                }
                return List.of();
            }

            @Override
            public List<Float> asFloatList() {
                if (obj instanceof List) {
                    List<?> list = (List<?>)obj;
                    return list.stream()
                            .mapToDouble(o -> wrapObject(o).asDouble())
                            .boxed()
                            .map(Double::floatValue)
                            .collect(Collectors.toList());
                }
                return List.of();
            }

            @Override
            public List<Double> asDoubleList() {
                if (obj instanceof List) {
                    List<?> list = (List<?>)obj;
                    return list.stream()
                            .mapToDouble(o -> wrapObject(o).asDouble())
                            .boxed()
                            .collect(Collectors.toList());
                }
                return List.of();
            }

            @Override
            public double[] asDoubleArray() {
                final int size = 256; // XXX
                if (obj instanceof List) {
                    List<?> list = (List<?>)obj;
                    return IntStream.range(0, size)
                            .mapToDouble(idx -> idx < list.size() ? (double) list.get(idx) : 0.0d)
                            .toArray();
                }
                return new double[size];
            }

            @Override
            public float[] asFloatArray() {
                return Value.super.asFloatArray();
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
                if (obj instanceof List) {
                    // XXX yolo
                    return Type.DOUBLE_ARRAY;
                }
                return Type.OBJECT;
            }
        };
    }

    @Override
    public Value get(int index) {
        return valueArray[index];
    }

    @Override
    public Value get(String field) {
        // XXX ugly
        for (int i=0; i<keyArray.length; i++) {
            if (keyArray[i].equals(field))
                return valueArray[i];
        }
        return null;
    }

    @Override
    public List<String> keys() {
        return Arrays.asList(keyArray);
    }

}
