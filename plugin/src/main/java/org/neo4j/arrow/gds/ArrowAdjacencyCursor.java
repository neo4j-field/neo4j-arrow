package org.neo4j.arrow.gds;

import org.neo4j.gds.api.AdjacencyCursor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class ArrowAdjacencyCursor implements AdjacencyCursor {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowAdjacencyCursor.class);

    private long pos = 0;

    private double fallbackValue = Double.NaN;
    private List<Integer> targets = List.of();
    private Function<Long, List<Integer>> queueResolver = (unused) -> new LinkedList<>();

    protected ArrowAdjacencyCursor(Function<Long, List<Integer>> queueResolver) {
        this.queueResolver = queueResolver;
    }

    protected ArrowAdjacencyCursor(List<Integer> targets, double fallbackValue) {
        this.targets = targets;
        this.fallbackValue = fallbackValue;
    }

    @Override
    public void init(long node, int unused) {
        pos = 0;
        targets = new ArrayList<>(queueResolver.apply(node));
    }

    @Override
    public int size() {
        return targets.size();
    }

    @Override
    public boolean hasNextVLong() {
        return (pos < targets.size());
    }

    @Override
    public long nextVLong() {
        try {
            final int targetIdx = targets.get((int) pos);
            pos++;
            return targetIdx;
        } catch (IndexOutOfBoundsException e) {
            logger.warn(String.format("nextVLong() index out of bounds (index=%d)", pos));
            return NOT_FOUND;
        }
    }

    @Override
    public long peekVLong() {
        if (hasNextVLong())
            return targets.get((int) pos); // XXX
        else
            return NOT_FOUND;
    }

    @Override
    public int remaining() {
        return (int) (targets.size() - pos);
    }

    @Override
    public long skipUntil(long nodeId) {
        long next = NOT_FOUND;
        while (hasNextVLong()) {
            next = nextVLong();
            if (next > nodeId)
                break;
        }
        return next;
    }

    @Override
    public long advance(long nodeId) {
        long next = NOT_FOUND;
        while (hasNextVLong()) {
            next = nextVLong();
            if (next >= nodeId)
                break;
        }
        return next;
    }

    @Override
    public AdjacencyCursor shallowCopy(AdjacencyCursor destination) {
        final AdjacencyCursor copy = (destination == null) ?
                new ArrowAdjacencyCursor(targets, fallbackValue)
                : destination;
        copy.init(pos, 0);
        return copy;
    }

    @Override
    public void close() {
        // nop
    }
}
