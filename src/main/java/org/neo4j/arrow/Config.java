package org.neo4j.arrow;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Super simple environment-based config.
 * <p>
 *     <i>Warning: the password is stored in the environment in plaintext!!!</i>
 * </p>
 */
public class Config {
    public final static Long TiB = (1L << 40);
    public final static Long GiB = (1L << 30);
    public final static Long MiB = (1L << 20);
    public final static Map<String, Long> scaleMap = Map.of(
            "T", TiB, "t", TiB,
            "G", GiB, "g", GiB,
            "M", MiB, "m", MiB,
            "", 1L
    );

    public static Long parseMemory(String memory) {
        return Stream.of(memory)
                .map(s -> {
                    if (!Character.isDigit(s.charAt(s.length() - 1))) {
                        return new String[] { s.substring(0, s.length() - 1), s.substring(s.length() - 1) };
                    }
                    return new String[] { s, "" };
                })
                .map(pair -> Long.parseLong(pair[0]) * scaleMap.get(pair[1]))
                .findFirst()
                .orElse(Long.MAX_VALUE);
    }

    /** Bolt URL for accessing a remote Neo4j database */
    public final static String neo4jUrl = System.getenv().getOrDefault("NEO4J_URL", "neo4j://localhost:7687");
    /** Username for any Neo4j driver connection */
    public final static String username = System.getenv().getOrDefault("NEO4J_USERNAME", "neo4j");
    /** Password for any Neo4j driver connection */
    public final static String password = System.getenv().getOrDefault("NEO4J_PASSWORD", "password");
    /** Name of the default Neo4j database to use */
    public final static String database = System.getenv().getOrDefault("NEO4J_DATABASE", "neo4j");

    /** Hostname or IP address to listen on when running as a server or connect to when a client */
    public final static String host = System.getenv().getOrDefault("HOST", "localhost");
    /** Port number to listen on or connect to. */
    public final static int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "9999"));

    /** Maximum native memory allowed to be allocated by the global allocator and its children */
    public final static long maxArrowMemory = parseMemory(
            System.getenv().getOrDefault("MAX_MEM_GLOBAL", Long.toString(Long.MAX_VALUE)));

    /** Maximum native memory allowed to be allocated by a single stream */
    public final static long maxStreamMemory = parseMemory(
            System.getenv().getOrDefault("MAX_MEM_STREAM",  Long.toString(Long.MAX_VALUE)));

    /** Arrow Batch Size controls the size of the transmitted vectors.*/
    public final static int arrowBatchSize = Math.abs(Integer.parseInt(
            System.getenv().getOrDefault("ARROW_BATCH_SIZE", Integer.toString(1_000))
    ));

    /** Arrow parallelism */
    public final static int arrowMaxPartitions = Math.abs(Integer.parseInt(
            System.getenv().getOrDefault("ARROW_MAX_PARTITIONS",
                    String.valueOf(Runtime.getRuntime().availableProcessors() > 2 ?
                        Runtime.getRuntime().availableProcessors() - 2 : 1))));

    /** Arrow flush timeout in seconds */
    public final static int arrowFlushTimeout = Math.abs(Integer.parseInt(
            System.getenv().getOrDefault("ARROW_FLUSH_TIMEOUT", Integer.toString(60 * 30))
    ));


    /** Bolt fetch size controls how many Records we PULL at a given time. Should be set lower
     * than the Arrow Batch size.
     */
    public final static long boltFetchSize = Math.abs(Long.parseLong(
            System.getenv().getOrDefault("BOLT_FETCH_SIZE", Long.toString(1_000))
    ));

    public final static String tlsCertficate = System.getenv().getOrDefault("ARROW_TLS_CERTIFICATE", "");
    public final static String tlsPrivateKey = System.getenv().getOrDefault("ARROW_TLS_PRIVATE_KEY", "");
}
