package org.neo4j.arrow.gds;

import org.neo4j.gds.api.Graph;

import java.util.Collection;

/**
 * Utilities for our k-hop implementation.
 */
public class KHop {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KHop.class);

    public static SuperNodeCache populateCache(Graph graph, Collection<Long> nodes) {
        // Pre-cache supernodes adjacency lists
        if (nodes.size() > 0) {
            logger.info("caching supernodes...");
            final SuperNodeCache supernodeCache = SuperNodeCache.ofSize(nodes.size());
            nodes.parallelStream()
                    .forEach(superNodeId ->
                            supernodeCache.set(superNodeId.intValue(),
                                    graph.concurrentCopy()
                                            .streamRelationships(superNodeId, Double.NaN)
                                            .mapToLong(cursor -> {
                                                final boolean isNatural = Double.isNaN(cursor.property());
                                                return Edge.edge(cursor.sourceId(), cursor.targetId(), isNatural);
                                            }).toArray()));

            logger.info(String.format("cached %,d supernodes (%,d edges)",
                    nodes.size(), supernodeCache.stream()
                            .mapToLong(array -> array == null ? 0 : array.length).sum()));
            return supernodeCache;
        }

        return SuperNodeCache.empty();
    }
}
