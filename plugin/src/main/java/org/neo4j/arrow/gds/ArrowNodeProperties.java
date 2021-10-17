package org.neo4j.arrow.gds;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.neo4j.arrow.ArrowBatch;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.*;
import java.util.function.Function;

public class ArrowNodeProperties implements NodeProperties {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowNodeProperties.class);

    final private ArrowBatch.BatchedVector vector;
    final private ValueType type;
    final private NodeLabel label;
    final int maxId;

    final Function<Integer, Value> valueReader;
    final Function<Integer, Number> numberReader;
    final Function<Integer, Number[]> arrayReader;

    public ArrowNodeProperties(ArrowBatch.BatchedVector vector, NodeLabel label, int maxId) {
        this.vector = vector;
        this.label = label;
        this.maxId = maxId;

        Class<?> clazz = vector.getBaseType();

        if (clazz.isAssignableFrom(BigIntVector.class) || clazz.isAssignableFrom(IntVector.class)) {
            this.type = ValueType.LONG;
            this.valueReader = (id) -> Values.longValue((Long) vector.getObject(id));
            this.numberReader = (id) -> (Long) vector.getObject(id);
            this.arrayReader = (unused) -> null;

        } else if (clazz.isAssignableFrom(Float4Vector.class) || clazz.isAssignableFrom(Float8Vector.class)) {
            this.type = ValueType.DOUBLE;
            this.valueReader = (id) -> Values.doubleValue((Double) vector.getObject(id));
            this.numberReader = (id) -> (Double) vector.getObject(id);
            this.arrayReader = (unused) -> null;

        } else if (clazz.isAssignableFrom(FixedSizeListVector.class) || clazz.isAssignableFrom(ListVector.class)) {
            // XXX this part is a mess...we need a better approach to types to avoid all the boxing/unboxing
            this.numberReader = (unused) -> -1L;
            final Class<?> dataClass = vector.getDataClass().get(); // XXX

            if (dataClass.isAssignableFrom(BigIntVector.class) || dataClass.isAssignableFrom(IntVector.class)) {
                this.type = ValueType.LONG_ARRAY;
                this.valueReader = (id) -> {
                    @SuppressWarnings("unchecked") final List<Long> list = (List<Long>) vector.getList(id); // XXX
                    return Values.longArray(list.stream().mapToLong(Long::longValue).toArray());
                };
                this.arrayReader = (id) -> {
                    @SuppressWarnings("unchecked") final List<Long> list = (List<Long>) vector.getList(id); // XXX
                    return list.stream().mapToLong(Long::valueOf).boxed().toArray(Number[]::new);
                };

            } else if (dataClass.isAssignableFrom(Float4Vector.class)) {
                this.type = ValueType.FLOAT_ARRAY;
                this.valueReader = (id) -> {
                    @SuppressWarnings("unchecked") final List<Float> list = (List<Float>) vector.getList(id); // XXX
                    return Values.doubleArray(list.stream().mapToDouble(Float::doubleValue).toArray());
                };
                this.arrayReader = (id) -> {
                    @SuppressWarnings("unchecked") final List<Float> list = (List<Float>) vector.getList(id); // XXX
                    return list.stream().mapToDouble(Float::valueOf).boxed().toArray(Number[]::new);
                };

            } else if (dataClass.isAssignableFrom(Float8Vector.class)) {
                this.type = ValueType.DOUBLE_ARRAY;
                this.valueReader = (id) -> {
                    @SuppressWarnings("unchecked") final List<Double> list = (List<Double>) vector.getList(id); // XXX
                    return Values.doubleArray(list.stream().mapToDouble(Double::doubleValue).toArray());
                };
                this.arrayReader = (id) -> {
                    @SuppressWarnings("unchecked") final List<Double> list = (List<Double>) vector.getList(id); // XXX
                    return list.stream().mapToDouble(Double::valueOf).boxed().toArray(Number[]::new);
                };

            } else {
                throw new RuntimeException(
                        String.format("unsupported NodeProperties list type in Arrow FieldVector: %s",
                                vector.getClass().getCanonicalName()));
            }
        } else {
            throw new RuntimeException(
                    String.format("unsupported NodeProperties type in Arrow FieldVector: %s", clazz));
        }
    }

    @Override
    public double doubleValue(long nodeId) {
        if (nodeId < maxId)
            return numberReader.apply((int) nodeId).doubleValue(); // XXX cast
        throw new RuntimeException("invalid node id (not in idMap)");
    }

    @Override
    public long longValue(long nodeId) {
        if (nodeId < maxId)
            return numberReader.apply((int) nodeId).longValue(); // XXX cast
        throw new RuntimeException("invalid node id (not in idMap)");    }

    @Override
    public
    double[] doubleArrayValue(long nodeId) {
        if (nodeId < maxId)
            return Arrays.stream(arrayReader.apply((int) nodeId)) // XXX cast
                    .mapToDouble(Number::doubleValue).toArray();
        throw new RuntimeException("invalid node id (not in idMap)");
    }

    @Override
    public
    float[] floatArrayValue(long nodeId) {
        if (nodeId < maxId) {
            final Number[] data = arrayReader.apply((int) nodeId); // XXX cast
            final float[] floats = new float[data.length];
            for (int i=0; i<floats.length; i++)
                floats[i] = data[i].floatValue();
            return floats;
        }
        throw new RuntimeException("invalid node id (not in idMap)");    }

    @Override
    public
    long[] longArrayValue(long nodeId) {
        if (nodeId < maxId)
            return Arrays.stream(arrayReader.apply((int) nodeId)) // XXX cast
                    .mapToLong(Number::longValue).toArray();
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
        if (nodeId < maxId)
            return valueReader.apply((int) nodeId); // XXX cast
        return Values.of(null);
    }

    @Override
    public long release() {
        logger.info("NodeProperties({}) released", vector.getName());
        //vector.close();
        return NodeProperties.super.release();
    }

    @Override
    public long size() {
        return 0;
    }

    public String getName() {
        return label.name();
    }

    @SuppressWarnings("unused")
    public NodeLabel getLabel() {
        return label;
    }
}
