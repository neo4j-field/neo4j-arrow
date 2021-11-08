package org.neo4j.arrow.gds;

import java.util.concurrent.ConcurrentSkipListSet;

public abstract class EdgeCache {
    public static EdgeCache of(long nodeCount) {
        if (nodeCount > 5_000_000) {
            // arbitrary cutoff for now
            return null;
        } else {
            return new SmallEdgeCache();
        }
    }

    public abstract boolean get(long edge);

    public abstract boolean getAndSet(long edge);

    public abstract void set(long edge);

    protected static class SmallEdgeCache extends EdgeCache {
        final ConcurrentSkipListSet<Long> set;

        protected SmallEdgeCache() {
            this.set = new ConcurrentSkipListSet<>();
        }

        @Override
        public boolean get(long edge) {
            return set.contains(edge);
        }

        @Override
        public boolean getAndSet(long edge) {
            final boolean isNewValue = set.add(edge);
            return !isNewValue;
        }

        @Override
        public void set(long edge) {
            set.add(edge);
        }
    }
}
