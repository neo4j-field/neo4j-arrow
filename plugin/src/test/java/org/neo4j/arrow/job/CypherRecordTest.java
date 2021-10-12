package org.neo4j.arrow.job;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.arrow.CypherRecord;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;

import java.util.List;

public class CypherRecordTest {
    private static final org.slf4j.Logger logger;

    static {
        // Set up nicer logging output.
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "[yyyy-MM-dd'T'HH:mm:ss:SSS]");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        logger = org.slf4j.LoggerFactory.getLogger(CypherRecordTest.class);
    }

    @Test
    public void canInferTypes() {
        Result.ResultRow row = new Result.ResultRow() {
            @Override
            public Node getNode(String key) {
                return null;
            }

            @Override
            public Relationship getRelationship(String key) {
                return null;
            }

            @Override
            public Object get(String key) {
                switch (key) {
                    case "int":
                        return 123;
                    case "bool":
                        return true;
                    case "string":
                        return "string";
                    case "float":
                        return 1.23f;
                    default:
                        return null;
                }
            }

            @Override
            public String getString(String key) {
                return "hey";
            }

            @Override
            public Number getNumber(String key) {
                return 123;
            }

            @Override
            public Boolean getBoolean(String key) {
                return true;
            }

            @Override
            public Path getPath(String key) {
                return null;
            }
        };

        final CypherRecord record = CypherRecord.wrap(row, List.of("int", "bool", "string", "float"));
        Assertions.assertEquals(RowBasedRecord.Type.INT, record.get("int").type());
        Assertions.assertEquals(RowBasedRecord.Type.FLOAT, record.get("float").type());
        Assertions.assertEquals(RowBasedRecord.Type.STRING, record.get("string").type());
    }
}
