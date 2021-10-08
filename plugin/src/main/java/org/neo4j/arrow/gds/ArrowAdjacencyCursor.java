package org.neo4j.arrow.gds;

import org.apache.arrow.vector.BigIntVector;
import org.neo4j.gds.api.AdjacencyCursor;

import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class ArrowAdjacencyCursor implements AdjacencyCursor {

    private long index = 0;
    // private int degree = 0;

    private final double fallbackValue;
    private final BigIntVector targetVector;
    private final List<Integer> outgoing;

    protected ArrowAdjacencyCursor(Queue<Integer> outgoing, BigIntVector targetVector, double fallbackValue) {
        this.outgoing = outgoing.stream().sorted().collect(Collectors.toList());
        this.targetVector = targetVector;
        this.fallbackValue = fallbackValue;
        // this.degree = outgoing.size();
    }

    private ArrowAdjacencyCursor(List<Integer> sortedOutgoing, BigIntVector targetVector, double fallbackValue) {
        this.outgoing = sortedOutgoing;
        this.targetVector = targetVector;
        this.fallbackValue = fallbackValue;
    }

    @Override
    public void init(long index, int unused) {
        this.index = index;
        // this.degree = degree; // XXX ???

    }

    @Override
    public int size() {
        return outgoing.size();
    }

    @Override
    public boolean hasNextVLong() {
        return (index < outgoing.size() - 1);
    }

    @Override
    public long nextVLong() {
        final long nodeId = targetVector.get(outgoing.get((int) index)); // XXX
        index++;
        return nodeId;
    }

    @Override
    public long peekVLong() {
        if (hasNextVLong())
            return targetVector.get(outgoing.get((int) index)); // XXX
        else
            return NOT_FOUND;
    }

    @Override
    public int remaining() {
        return (int) (outgoing.size() - index + 1);
    }

    @Override
    public long skipUntil(long nodeId) {
        while (hasNextVLong()) {
            if (nextVLong() > nodeId)
                break;
        }
        return NOT_FOUND;
    }

    @Override
    public long advance(long nodeId) {
        while (hasNextVLong()) {
            if (nextVLong() >= nodeId)
                break;
        }
        return NOT_FOUND;    }

    @Override
    public AdjacencyCursor shallowCopy(AdjacencyCursor destination) {
        final AdjacencyCursor copy = (destination == null) ?
                new ArrowAdjacencyCursor(outgoing, targetVector, fallbackValue)
                : destination;
        copy.init(index, 0);
        return copy;
    }

    @Override
    public void close() {
        // nop
    }
}
