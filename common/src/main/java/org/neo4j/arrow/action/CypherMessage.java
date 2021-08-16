package org.neo4j.arrow.action;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.parser.CypherParser;
import org.neo4j.cypher.internal.util.CypherExceptionFactory;
import org.neo4j.cypher.internal.util.InputPosition;
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory;
import scala.Option;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CypherMessage {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CypherMessage.class);

    static private final ObjectMapper mapper = new ObjectMapper();
    static private final CypherParser parser = new CypherParser();

    private final String cypher;
    private final String database;
    private final Map<String, Object> params;

    public CypherMessage(String database, String cypher) {
        this(database, cypher, Map.of());
    }

    public CypherMessage(String database, String cypher, Map<String, Object> params) {
        this.cypher = cypher;
        this.params = params;
        this.database = database;

        Statement stmt = parser.parse(cypher, new CypherExceptionFactory() {
            @Override
            public Exception arithmeticException(String message, Exception cause) {
                logger.error(message, cause);
                return cause;
            }

            @Override
            public Exception syntaxException(String message, InputPosition pos) {
                Exception e = new OpenCypherExceptionFactory.SyntaxException(message, pos);
                logger.error(message, e);
                return e;
            }
        }, Option.apply(InputPosition.NONE()));

        logger.info("parsed: {}", stmt.asCanonicalStringVal());
    }

    public static CypherMessage deserialize(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes)
                .asReadOnlyBuffer()
                .order(ByteOrder.BIG_ENDIAN);

        short len = buffer.getShort();
        byte[] slice = new byte[len];
        buffer.get(slice);
        String cypher = new String(slice, StandardCharsets.UTF_8);

        len = buffer.getShort();
        slice = new byte[len];
        buffer.get(slice);
        String database = new String(slice, StandardCharsets.UTF_8);

        len = buffer.getShort();
        slice = new byte[len];
        buffer.get(slice);
        Map<String, Object> params = mapper.createParser(slice).readValueAs(Map.class);

        return new CypherMessage(database, cypher, params);
    }

    /**
     * Serialize the CypherMessage to a platform independent format, kept very simple for now, in
     * network byte order.
     *   [0-3] - length of cypher string C
     *   [4-C] - cypher string
     *   [C-C+4] - length of database name D
     *   [C+4-C+D+4] - database name string
     *   [C+D+4-C+D+8] - length of params P as JSON
     *   [C+D+8-C+D+P+8] - params serialized as JSO
     *
     * @return byte[]
     */
    public byte[] serialize() {
        // not most memory sensitive approach for now, but simple
        try {
            final byte[] cypherBytes = cypher.getBytes(StandardCharsets.UTF_8);
            final byte[] databaseBytes = database.getBytes(StandardCharsets.UTF_8);
            final byte[] paramsBytes = mapper.writeValueAsString(params).getBytes(StandardCharsets.UTF_8);

            ByteBuffer buffer = ByteBuffer.allocate(cypherBytes.length + paramsBytes.length + 12);
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.putShort((short) cypherBytes.length);
            buffer.put(cypherBytes);
            buffer.putShort((short) databaseBytes.length);
            buffer.put(databaseBytes);
            buffer.putShort((short) paramsBytes.length);
            buffer.put(paramsBytes);
            return buffer.array();
        } catch (Exception e) {
            logger.error("serialization error", e);
            e.printStackTrace();
        }
        return new byte[0];
    }

    public String getCypher() {
        return cypher;
    }

    public String getDatabase() {
        return database;
    }

    public Map<String, Object> getParams() {
        return params;
    }
}
