package org.neo4j.arrow.gds;

import org.apache.arrow.vector.BigIntVector;
import org.neo4j.gds.api.AdjacencyCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class ArrowAdjacencyCursor implements AdjacencyCursor {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowAdjacencyCursor.class);

    private long index = 0;
    // private int degree = 0;

    private final double fallbackValue;
    private final BigIntVector targetVector;
    private final List<Integer> targets;

    protected ArrowAdjacencyCursor(Queue<Integer> targets, BigIntVector targetVector, double fallbackValue) {
        this(new ArrayList<>(targets), targetVector, fallbackValue);
    }

    private ArrowAdjacencyCursor(List<Integer> sortedOutgoing, BigIntVector targetVector, double fallbackValue) {
        this.targets = sortedOutgoing;
        this.targetVector = targetVector;
        this.fallbackValue = fallbackValue;
        logger.trace("new cursor (outgoing: {}, targetVector: {}, fallbackValue: {})", sortedOutgoing, targetVector, fallbackValue);
    }

    @Override
    public void init(long index, int unused) {
        this.index = index;
        logger.trace("init: {}, {}", index, unused);
    }

    @Override
    public int size() {
        return targets.size();
    }

    @Override
    public boolean hasNextVLong() {
        return (index < targets.size());
    }

    @Override
    public long nextVLong() {
        final int targetIdx = targets.get((int) index);
        final long nodeId = targetVector.get(targetIdx); // XXX
        logger.trace("nextVLong: @{}, offset: {}, nodeId: {}", index, targetIdx, nodeId);
        index++;
        return nodeId;
    }

    @Override
    public long peekVLong() {
        if (hasNextVLong())
            return targetVector.get(targets.get((int) index)); // XXX
        else
            return NOT_FOUND;
    }

    @Override
    public int remaining() {
        return (int) (targets.size() - index + 1);
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
                new ArrowAdjacencyCursor(targets, targetVector, fallbackValue)
                : destination;
        copy.init(index, 0);
        return copy;
    }

    @Override
    public void close() {
        // nop
    }
}
