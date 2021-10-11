package org.neo4j.arrow;

import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Wrapper around a record of Nodes and properties from the in-memory graph in GDS.
 * <p>
 *     GDS gives us the ability to use fixed-width data structures, specifically certain Java
 *     primitives. This dramatically increases the efficiency and performance of Apache Arrow. As
 *     such, we use as many Java arrays as we can in lieu of boxed types in {@link List}s.
 * </p>
 * <p>TODO: Currently no label support :-(</p>
 */
public class GdsNodeRecord extends GdsRecord {
    /** Represents the underlying node id */
    private final Value nodeId;

    public static final String NODE_ID_FIELD = "node_id";

    protected GdsNodeRecord(long nodeId, String[] keys, Value[] values, Function<Long, Long> nodeIdResolver) {
        super(keys, values);
        this.nodeId = wrapScalar(nodeIdResolver.apply(nodeId), ValueType.LONG);
    }

    // TODO: LABELS!!??!
    public static GdsNodeRecord wrap(long nodeId, String[] keys, Value[] values) {
        return new GdsNodeRecord(nodeId, keys, values, Function.identity());
    }

    /**
     * Wrap the given GDS information into a single {@link GdsNodeRecord}.
     *
     * @param nodeId the native node id of the record
     * @param fieldNames the names of the properties or fields
     * @param propertiesArray an array of references to the {@link NodeProperties} interface for
     *                        resolving the property values
     * @return a new {@link GdsNodeRecord}
     */
    public static GdsNodeRecord wrap(long nodeId, String[] fieldNames, NodeProperties[] propertiesArray,
                                     Function<Long, Long> nodeIdResolver) {
        final Value[] values = new Value[propertiesArray.length];

        assert fieldNames.length == values.length;

        for (int i=0; i<fieldNames.length; i++) {
            final NodeProperties properties = propertiesArray[i];
            Value value;
            switch (properties.valueType()) {
                // TODO: INT? Does it exist?
                case LONG:
                    value = wrapScalar(properties.longValue(nodeId), properties.valueType());
                    break;
                case DOUBLE:
                    value = wrapScalar(properties.doubleValue(nodeId), properties.valueType());
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
        return new GdsNodeRecord(nodeId, fieldNames, values, nodeIdResolver);
    }

    @Override
    public Value get(int index) {
        if (index < 0 || index > valueArray.length)
            throw new RuntimeException("invalid index");
        if (index == 0)
            return nodeId;
        return valueArray[index - 1];
    }

    @Override
    public Value get(String field) {
        if (NODE_ID_FIELD.equals(field)) {
            return nodeId;
        } else {
            for (int i = 0; i < keyArray.length; i++)
                if (keyArray[i].equals(field))
                    return valueArray[i];
        }
        throw new RuntimeException("invalid field");
    }

    @Override
    public List<String> keys() {
        ArrayList<String> list = new ArrayList<>();
        list.add(NODE_ID_FIELD);
        list.addAll(List.of(keyArray));
        return list;
    }

}
