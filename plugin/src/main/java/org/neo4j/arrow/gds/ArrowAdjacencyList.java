package org.neo4j.arrow.gds;

import org.apache.arrow.vector.BigIntVector;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;

import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

public class ArrowAdjacencyList implements AdjacencyList {

    final private Map<Long, Queue<Integer>> sourceIdMap;
    final private Map<Long, Queue<Integer>> targetIdMap;

    final private BigIntVector sourceIdVector;
    final private BigIntVector targetIdVector;

    final private Consumer<Void> closeCallback;

    public ArrowAdjacencyList(Map<Long, Queue<Integer>> sourceIdMap, Map<Long, Queue<Integer>> targetIdMap,
                              BigIntVector sourceIdVector, BigIntVector targetIdVector, Consumer<Void> closeCallback) {
        this.sourceIdMap = sourceIdMap;
        this.targetIdMap = targetIdMap;
        this.sourceIdVector = sourceIdVector;
        this.targetIdVector = targetIdVector;
        this.closeCallback = closeCallback;
    }

    @Override
    public int degree(long node) {
        return (sourceIdMap.containsKey(node) ? sourceIdMap.get(node).size() : 0)
                + (targetIdMap.containsKey(node) ? targetIdMap.get(node).size() : 0);
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long node) {
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), targetIdVector, Double.NaN)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), targetIdVector, fallbackValue)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;    }

    @Override
    public AdjacencyCursor adjacencyCursor(AdjacencyCursor reuse, long node) {
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), targetIdVector, Double.NaN)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;    }

    @Override
    public AdjacencyCursor adjacencyCursor(AdjacencyCursor reuse, long node, double fallbackValue) {
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), targetIdVector, Double.NaN)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        // XXX ???
        return AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
    }

    @Override
    public void close() {
        closeCallback.accept(null);
    }
}
