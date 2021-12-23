package org.neo4j.arrow.action;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BulkImportMessage implements Message {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BulkImportMessage.class);
    static private final ObjectMapper mapper = new ObjectMapper();

    public static final String JSON_KEY_DATABASE_NAME = "db";

    public static final String JSON_KEY_NODE_ID_FIELD = "idField";
    public static final String DEFAULT_NODE_ID_FIELD = "_id_";

    public static final String JSON_KEY_LABELS_FIELD = "labelsField";
    public static final String DEFAULT_LABELS_FIELD = "_labels_";

    public static final String JSON_KEY_SOURCE_FIELD = "sourceField";
    public static final String DEFAULT_SOURCE_FIELD = "_source_id_";

    public static final String JSON_KEY_TARGET_FIELD = "targetField";
    public static final String DEFAULT_TARGET_FIELD = "_target_id_";

    public static final String JSON_KEY_TYPE_FIELD = "typeField";
    public static final String DEFAULT_TYPE_FIELD = "_type_";

    private final String dbName;
    private final String idField;
    private final String labelsField;
    private final String sourceField;
    private final String targetField;
    private final String typeField;

    private String eitherOrDefault(String input, String defaultValue) {
        if (input == null || input.isEmpty())
            return defaultValue;
        return input;
    }

    public BulkImportMessage(String dbName, String idField, String labelsField,
                             String sourceField, String targetField, String typeField) {
        this.dbName = dbName;
        this.idField = eitherOrDefault(idField, DEFAULT_NODE_ID_FIELD);
        this.labelsField = eitherOrDefault(labelsField, DEFAULT_LABELS_FIELD);
        this.sourceField = eitherOrDefault(sourceField, DEFAULT_SOURCE_FIELD);
        this.targetField = eitherOrDefault(targetField, DEFAULT_TARGET_FIELD);
        this.typeField = eitherOrDefault(typeField, DEFAULT_TYPE_FIELD);
    }

    @Override
    public byte[] serialize() {
        try {
            return mapper.writeValueAsString(
                            Map.of(JSON_KEY_DATABASE_NAME, dbName,
                                    JSON_KEY_NODE_ID_FIELD, idField,
                                    JSON_KEY_LABELS_FIELD, labelsField,
                                    JSON_KEY_SOURCE_FIELD, sourceField,
                                    JSON_KEY_TARGET_FIELD, targetField,
                                    JSON_KEY_TYPE_FIELD, typeField))
                    .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            logger.error("serialization error", e);
        }
        return new byte[0];
    }

    private static class MapTypeReference extends TypeReference<Map<String, Object>> { }

    public static BulkImportMessage deserialize(byte[] bytes) throws IOException {
        final JsonParser parser = mapper.createParser(bytes);
        final Map<String, Object> params = parser.readValueAs(new BulkImportMessage.MapTypeReference());

        final String dbName = params.getOrDefault(JSON_KEY_DATABASE_NAME, "neo4j").toString();

        final String idField = params.get(JSON_KEY_NODE_ID_FIELD) != null ?
                params.get(JSON_KEY_NODE_ID_FIELD).toString() : null;

        final String labelsField = params.get(JSON_KEY_LABELS_FIELD) != null ?
                params.get(JSON_KEY_LABELS_FIELD).toString() : null;

        final String sourceField = params.get(JSON_KEY_SOURCE_FIELD) != null ?
                params.get(JSON_KEY_SOURCE_FIELD).toString() : null;

        final String targetField = params.get(JSON_KEY_TARGET_FIELD) != null ?
                params.get(JSON_KEY_TARGET_FIELD).toString() : null;

        final String typeField = params.get(JSON_KEY_TYPE_FIELD) != null ?
                params.get(JSON_KEY_TYPE_FIELD).toString() : null;

        return new BulkImportMessage(dbName, idField, labelsField, sourceField,
                targetField, typeField);

    }

    public String getDbName() {
        return dbName;
    }

    public String getIdField() {
        return idField;
    }

    public String getLabelsField() {
        return labelsField;
    }

    public String getSourceField() {
        return sourceField;
    }

    public String getTargetField() {
        return targetField;
    }

    public String getTypeField() {
        return typeField;
    }
}
