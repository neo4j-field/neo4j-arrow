package org.neo4j.arrow.gds;

/**
 * Optimized relationship format in 64-bits
 */
public class Edge {
    public static final int BITS_META = 4;
    public static final int BITS_NODE = 30;
    public static final int SIZE = 64;

    public static final long ZERO = 0x0L;
    public static final long ORIENTATION_BIT = 0x8000000000000000L;
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

    public static int sourceAsInt(long edge) {
        return (int) source(edge);
    }

    public static long target(long edge) {
        return (edge & TARGET_BITS);
    }

    public static int targetAsInt(long edge) {
        return (int) target(edge);
    }

    public static long uniquify(long edge) {
        // convert an edge to a "unique" form...which for now just means flipping direction if it's reversed
        return flag(edge) ? (edge & FLAG_MASK) : edge(target(edge), source(edge));
    }

    public static boolean flag(long edge) {
        // TODO: use all 4 bits
        return (edge >> 60) != 0;
    }

    public static boolean isNatural(long edge) {
        // 1 for natural, 0 for reversed
        return (edge & ORIENTATION_BIT) != 0;
    }

    public static int realTarget(long edge) {
        return isNatural(edge) ? targetAsInt(edge) : sourceAsInt(edge);
    }

    public static int realSource(long edge) {
        return isNatural(edge) ? sourceAsInt(edge) : targetAsInt(edge);
    }
}
