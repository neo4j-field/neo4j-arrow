package org.neo4j.arrow.gds;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.*;
import java.util.function.Function;

public class ArrowNodeProperties implements NodeProperties {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowNodeProperties.class);

    final private FieldVector vector;
    final private ValueType type;
    final private Map<Long, Integer> idMap;
    final private NodeLabel label;

    final Function<Integer, Value> valueReader;
    final Function<Integer, Number> numberReader;
    final Function<Integer, Number[]> arrayReader;

    public ArrowNodeProperties(FieldVector vector, NodeLabel label, Map<Long, Integer> idMap) {
        this.vector = vector;
        this.idMap = idMap;
        this.label = label;

        if (vector instanceof BigIntVector || vector instanceof IntVector) {
            this.type = ValueType.LONG;
            this.valueReader = (id) -> Values.longValue(((BaseIntVector) vector).getValueAsLong(id));
            this.numberReader = ((BaseIntVector) vector)::getValueAsLong;
            this.arrayReader = (unused) -> null;

        } else if (vector instanceof Float4Vector || vector instanceof Float8Vector) {
            this.type = ValueType.DOUBLE;
            this.valueReader = (id) -> Values.doubleValue(((FloatingPointVector) vector).getValueAsDouble(id));
            this.numberReader = ((FloatingPointVector) vector)::getValueAsDouble;
            this.arrayReader = (unused) -> null;

        } else if (vector instanceof ListVector) {
            this.numberReader = (unused) -> null;

            final FieldVector dataVector = ((ListVector) vector).getDataVector();
            if (dataVector instanceof BigIntVector || dataVector instanceof IntVector) {
                this.type = ValueType.LONG_ARRAY;
                this.valueReader = (id) -> {
                    final List<Long> list = (List<Long>) vector.getObject(id); // XXX
                    return Values.longArray(list.stream().mapToLong(Long::longValue).toArray());
                };
                this.arrayReader = (id) -> {
                    final List<Long> list = (List<Long>) vector.getObject(id); // XXX
                    return list.stream().mapToLong(Long::valueOf).boxed().toArray(Number[]::new);
                };

            } else if (dataVector instanceof Float4Vector) {
                this.type = ValueType.FLOAT_ARRAY;
                this.valueReader = (id) -> {
                    final List<Float> list = (List<Float>) vector.getObject(id); // XXX
                    return Values.doubleArray(list.stream().mapToDouble(Float::doubleValue).toArray());
                };
                this.arrayReader = (id) -> {
                    final List<Float> list = (List<Float>) vector.getObject(id); // XXX
                    return list.stream().mapToDouble(Float::valueOf).boxed().toArray(Number[]::new);
                };

            } else if (dataVector instanceof Float8Vector) {
                this.type = ValueType.DOUBLE_ARRAY;
                this.valueReader = (id) -> {
                    final List<Double> list = (List<Double>) vector.getObject(id); // XXX
                    return Values.doubleArray(list.stream().mapToDouble(Double::doubleValue).toArray());
                };
                this.arrayReader = (id) -> {
                    final List<Double> list = (List<Double>) vector.getObject(id); // XXX
                    return list.stream().mapToDouble(Double::valueOf).boxed().toArray(Number[]::new);
                };

            } else {
                throw new RuntimeException(
                        String.format("unsupported NodeProperties list type in Arrow FieldVector: %s",
                                vector.getClass().getCanonicalName()));
            }
        } else {
            throw new RuntimeException(
                    String.format("unsupported NodeProperties type in Arrow FieldVector: %s",
                    vector.getClass().getCanonicalName()));
        }
    }

    @Override
    public double doubleValue(long nodeId) {
        if (idMap.containsKey(nodeId))
            return numberReader.apply(idMap.get(nodeId)).doubleValue();
        throw new RuntimeException("invalid node id (not in idMap)");
    }

    @Override
    public long longValue(long nodeId) {
        if (idMap.containsKey(nodeId))
            return numberReader.apply(idMap.get(nodeId)).longValue();
        throw new RuntimeException("invalid node id (not in idMap)");    }

    @Override
    public
    double[] doubleArrayValue(long nodeId) {
        if (idMap.containsKey(nodeId))
            return Arrays.stream(arrayReader.apply(idMap.get(nodeId))).mapToDouble(Number::doubleValue).toArray();
        throw new RuntimeException("invalid node id (not in idMap)");
    }

    @Override
    public
    float[] floatArrayValue(long nodeId) {
        if (idMap.containsKey(nodeId)) {
            final Number[] data = arrayReader.apply(idMap.get(nodeId));
            final float[] floats = new float[data.length];
            for (int i=0; i<floats.length; i++)
                floats[i] = data[i].floatValue();
            return floats;
        }
        throw new RuntimeException("invalid node id (not in idMap)");    }

    @Override
    public
    long[] longArrayValue(long nodeId) {
        if (idMap.containsKey(nodeId))
            return Arrays.stream(arrayReader.apply(idMap.get(nodeId))).mapToLong(Number::longValue).toArray();
        throw new RuntimeException("invalid node id (not in idMap)");    }

    @Override
    public OptionalLong getMaxLongPropertyValue() {
        return NodeProperties.super.getMaxLongPropertyValue();
    }

    @Override
    public OptionalDouble getMaxDoublePropertyValue() {
        return NodeProperties.super.getMaxDoublePropertyValue();
    }

    @Override
    public Object getObject(long nodeId) {
        assert(0 <= nodeId && nodeId <= Integer.MAX_VALUE);
        return value(nodeId).asObject();
    }

    @Override
    public ValueType valueType() {
        return type;
    }

    @Override
    public Value value(long nodeId) {
        final Integer idx = idMap.get(nodeId);
        if (idx == null) {
            return Values.of(null);
        }
        return valueReader.apply(idx);
    }

    @Override
    public long release() {
        logger.info("NodeProperties({}) released", vector.getName());
        vector.close();
        return NodeProperties.super.release();
    }

    @Override
    public long size() {
        return 0;
    }

    public String getName() {
        return label.name();
    }

    public NodeLabel getLabel() {
        return label;
    }
}
