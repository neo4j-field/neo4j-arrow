package org.neo4j.arrow.gds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Simple wrapper class to let us experiment with differing approaches to implementing
 * a supernode cache.
 */
public class SuperNodeCache {
    private final Map<Integer, long[]> cache;

    private SuperNodeCache(int size) {
        if (size > 0) {
            cache = new ConcurrentHashMap<>(size);
        } else {
            cache = Map.of();
        }
    }

    public static SuperNodeCache empty() {
        return new SuperNodeCache(0);
    }

    public static SuperNodeCache ofSize(int size) {
        return new SuperNodeCache(size);
    }

    public long[] get(int index) {
        if (cache != null) {
            return cache.get(index);
        }
        return null;
    }

    public void set(int index, long[] value) {
        cache.put(index, value);
    }

    public Stream<long[]> stream() {
        return cache.values().stream();
    }
}