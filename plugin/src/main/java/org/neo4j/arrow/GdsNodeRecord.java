package org.neo4j.arrow;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    protected GdsNodeRecord(long nodeId, String[] keys, Value[] values) {
        super(keys, values);
        this.nodeId = wrapScalar(ValueType.LONG, nodeId);
    }

    protected GdsNodeRecord(long nodeId, List<String> keys, List<Value> values) {
        this(nodeId, keys.toArray(new String[0]), values.toArray(new Value[0]));
    }

    /**
     * Wrap the given GDS information into a single {@link GdsNodeRecord}.
     *
     * @param nodeId the native node id of the record
     * @param fieldName the name of the property or field
     * @param properties a reference to the {@link NodeProperties} interface for resolving the value
     * @return a new {@link GdsNodeRecord}
     */
    public static GdsNodeRecord wrap(long nodeId, String fieldName, NodeProperties properties) {
        return wrap(nodeId, new String[] { fieldName }, new NodeProperties[] { properties });
    }

    public static GdsNodeRecord wrap(long nodeId, List<String> fieldNames, List<NodeProperties> propertiesList) {
        return wrap(nodeId, fieldNames.toArray(new String[0]),
                propertiesList.toArray(new NodeProperties[0]));
    }

    public static GdsNodeRecord wrap(long nodeId, String[] fieldNames, NodeProperties[] propertiesArray) {
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
        return new GdsNodeRecord(nodeId, fieldNames, values);
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
        if ("nodeId".equals(field)) {
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
        ArrayList<String> list = new ArrayList<>(keyArray.length + 1);
        list.add("nodeId");
        list.addAll(List.of(keyArray));
        return list;
    }

}
