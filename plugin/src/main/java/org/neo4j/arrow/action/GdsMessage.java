package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Primary message type for communicating parameters to GDS jobs. Relies on JSON for serialization
 * to keep things simple and portable without manual byte-packing, etc.
 */
public class GdsMessage implements Message {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GdsMessage.class);

    static private final ObjectMapper mapper = new ObjectMapper();

    public enum RequestType {
        node("node"),
        relationship("relationship"),
        khop("khop");

        final String type;

        RequestType(String type) {
            this.type = type;
        }
    }

    public static final String JSON_KEY_DATABASE_NAME = "db";
    public static final String JSON_KEY_GRAPH_NAME = "graph";
    // TODO: "type" is pretty vague...needs a better name
    public static final String JSON_KEY_REQUEST_TYPE = "type";
    public static final String JSON_KEY_FILTER_LIST = "filters";
    public static final String JSON_KEY_PROPERTY_LIST = "properties";

    /** Name of the Neo4j Database where our Graph lives. Optional. Defaults to "neo4j" */
    private final String dbName;
    /** Name of the Graph (projection) from the Graph Catalog. Required. */
    private final String graphName;
    /** Type of data request: either "node" or "relationship". */
    private final RequestType requestType;
    /** List of "filters" to apply to the Graph when generating results. Optional. */
    private final List<String> filters;
    /** List of properties to read or write. Optional for reads (Can be empty for "all" properties
     * during reads.)
     */
    private final List<String> properties;

    /** Special instance to use as a way for us to "fail open" with properties */
    public static final List<String> ANY_PROPERTIES = List.of();

    public GdsMessage(String dbName, String graphName, RequestType requestType, List<String> properties, List<String> filters) {
        this.dbName = dbName;
        this.graphName = graphName;
        this.requestType = requestType;
        this.properties = properties;
        this.filters = filters;

        // TODO: validation / constraints of values?
    }

    /**
     * Serialize the GdsMessage to a JSON blob of bytes.
     * @return byte[] containing UTF-8 JSON blob or byte[0] on error
     */
    public byte[] serialize() {
        try {
            return mapper.writeValueAsString(
                    Map.of(JSON_KEY_DATABASE_NAME, dbName,
                            JSON_KEY_GRAPH_NAME, graphName,
                            JSON_KEY_REQUEST_TYPE, requestType,
                            JSON_KEY_FILTER_LIST, filters,
                            JSON_KEY_PROPERTY_LIST, properties))
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("serialization error", e);
        }
        return new byte[0];
    }

    private static class MapTypeReference extends TypeReference<Map<String, Object>> { }

    /**
     * Deserialize the given bytes, containing JSON, into a GdsMessage instance.
     * @param bytes UTF-8 bytes containing JSON payload
     * @return new GdsMessage
     * @throws IOException if error encountered during serialization
     */
    public static GdsMessage deserialize(byte[] bytes) throws IOException {

        final JsonParser parser = mapper.createParser(bytes);
        final Map<String, Object> params = parser.readValueAs(new MapTypeReference());

        final String dbName = params.getOrDefault(JSON_KEY_DATABASE_NAME, "neo4j").toString();
        // TODO: assert our minimum schema?
        final String graphName = params.get(JSON_KEY_GRAPH_NAME).toString();

        final RequestType requestType = RequestType.valueOf(
                params.getOrDefault(JSON_KEY_REQUEST_TYPE, RequestType.node).toString());

        List<String> filters = List.of();
        Object obj = params.getOrDefault(JSON_KEY_FILTER_LIST, filters);
        if (obj instanceof List) {
            filters = ((List<?>)obj).stream().map(Object::toString).collect(Collectors.toList());
        }

        List<String> properties = List.of();
        obj = params.getOrDefault(JSON_KEY_PROPERTY_LIST, properties);
        if (obj instanceof List) {
            properties = ((List<?>)obj).stream().map(Object::toString).collect(Collectors.toList());
        }
        if (properties.isEmpty())
            properties = ANY_PROPERTIES;

        return new GdsMessage(dbName, graphName, requestType, properties, filters);
    }

    public String getDbName() {
        return dbName;
    }

    public String getGraphName() {
        return graphName;
    }

    public RequestType getRequestType() { return requestType; }

    public List<String> getFilters() {
        return filters;
    }

    public List<String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "GdsMessage{" +
                "db='" + dbName + '\'' +
                ", graph='" + graphName + '\'' +
                ", type='" + requestType + '\'' +
                ", filters=" + filters +
                ", properties=" + (properties == ANY_PROPERTIES ? "ANY" : properties) +
                '}';
    }
}
