package org.neo4j.arrow;

/**
 * Super simple environment-based config. Password is cleartext in the process's environment, so
 * BUYER BEWARE!
 */
public class Config {
    public final static String neo4jUrl = System.getenv().getOrDefault("NEO4J_URL", "neo4j://localhost:7687");
    public final static String username = System.getenv().getOrDefault("NEO4J_USERNAME", "neo4j");
    public final static String password = System.getenv().getOrDefault("NEO4J_PASSWORD", "password");
    public final static String database = System.getenv().getOrDefault("NEO4J_DATABASE", "neo4j");

    /* Hostname or IP address to listen on when running as a server or connect to when a client */
    public final static String host = System.getenv().getOrDefault("HOST", "localhost");
    /* Port number to listen on or connect to. */
    public final static int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "9999"));

    /* Maximum native memory allowed to be allocated by the global allocator and its children */
    public final static long maxGlobalMemory = Long.parseLong(
            System.getenv().getOrDefault("MAX_MEM_GLOBAL", Long.toString(Long.MAX_VALUE)));
    /* Maximum native memory allowed to be allocated by a single stream */
    public final static long maxStreamMemory = Long.parseLong(
            System.getenv().getOrDefault("MAX_MEM_STREAM", Long.toString(Integer.MAX_VALUE)));
}
