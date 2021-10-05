package org.neo4j.arrow.job;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ArrowNodeProperties implements NodeProperties {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowNodeProperties.class);

    final private FieldVector vector;
    final private ValueType type;
    final private Map<Long, Integer> idMap;
    final private NodeLabel label;

    final Function<Integer, Value> valueReader;

    public ArrowNodeProperties(FieldVector vector, NodeLabel label, Map<Long, Integer> idMap) {
        this.vector = vector;
        this.idMap = idMap;
        this.label = label;

        if (vector instanceof BigIntVector || vector instanceof IntVector) {
            this.type = ValueType.LONG;
            this.valueReader = (id) -> Values.longValue(((BaseIntVector) vector).getValueAsLong(id));

        } else if (vector instanceof Float4Vector || vector instanceof Float8Vector) {
            this.type = ValueType.DOUBLE;
            this.valueReader = (id) -> Values.doubleValue(((FloatingPointVector) vector).getValueAsDouble(id));

        } else if (vector instanceof ListVector) {
            final FieldVector dataVector = ((ListVector) vector).getDataVector();
            if (dataVector instanceof BigIntVector || dataVector instanceof IntVector) {
                this.type = ValueType.LONG_ARRAY;
                this.valueReader = (id) -> {
                    List<Long> list = (List<Long>) vector.getObject(id); // XXX
                    return Values.longArray(list.stream().mapToLong(Long::longValue).toArray());
                };
            } else if (dataVector instanceof Float4Vector) {
                this.type = ValueType.FLOAT_ARRAY;
                this.valueReader = (id) -> {
                    final List<Float> list = (List<Float>) vector.getObject(id); // XXX
                    return Values.doubleArray(list.stream().mapToDouble(Float::doubleValue).toArray());
                };
            } else if (dataVector instanceof Float8Vector) {
                this.type = ValueType.DOUBLE_ARRAY;
                this.valueReader = (id) -> {
                    final List<Double> list = (List<Double>) vector.getObject(id); // XXX
                    return Values.doubleArray(list.stream().mapToDouble(Double::doubleValue).toArray());
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
    public Object getObject(long nodeId) {
        assert(0 <= nodeId && nodeId <= Integer.MAX_VALUE);
        return vector.getObject((int) nodeId);
    }

    @Override
    public ValueType valueType() {
        return type;
    }

    @Override
    public Value value(long nodeId) {
        final Integer idx =  idMap.get(nodeId);
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
