package org.neo4j.arrow.gds;

/**
 * Optimized relationship format in 64-bits
 */
public class Edge {
    public static final long ZERO = 0x0L;
    public static final long FLAG_BITS = 0xF000000000000000L;
    public static final long FLAG_MASK = 0x0FFFFFFFFFFFFFFFL;
    public static final long TARGET_BITS = 0x000000003FFFFFFFL;

    public static long edge(long source, long target) {
        return (source << 30) | target;
    }

    public static long edge(long source, long target, boolean flag) {
        // assumption: source and target are both < 30 bits in length
        return ((source << 30) | target) | (flag ? FLAG_BITS : ZERO);
    }

    public static long source(long edge) {
        return (edge >> 30) & TARGET_BITS;
    }

    public static long target(long edge) {
        return (edge & TARGET_BITS);
    }

    public static long uniquify(long edge) {
        // convert an edge to a "unique" form...which for now just means flipping direction if it's reversed
        return flag(edge) ? (edge & FLAG_MASK) : edge(target(edge), source(edge));
    }

    public static boolean flag(long edge) {
        // TODO: use all 4 bits
        return (edge >> 60) != 0;
    }
}
