package org.neo4j.arrow.gds;

import org.apache.arrow.vector.BigIntVector;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;

import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

public class ArrowAdjacencyList implements AdjacencyList {

    final private Map<Integer, Queue<Integer>> sourceIdMap;
    final private Consumer<Void> closeCallback;

    public ArrowAdjacencyList(Map<Integer, Queue<Integer>> sourceIdMap, Consumer<Void> closeCallback) {
        this.sourceIdMap = sourceIdMap;
        this.closeCallback = closeCallback;
    }

    @Override
    public int degree(long longNode) {
        final int node = (int)longNode; // XXX
        return (sourceIdMap.containsKey(node) ? sourceIdMap.get(node).size() : 0);
                //+ (targetIdMap.containsKey(node) ? targetIdMap.get(node).size() : 0);
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long longNode) {
        final int node = (int)longNode; // XXX
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), Double.NaN)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long longNode, double fallbackValue) {
        final int node = (int)longNode; // XXX
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), fallbackValue)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(AdjacencyCursor reuse, long longNode) {
        final int node = (int)longNode; // XXX
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), Double.NaN)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(AdjacencyCursor reuse, long longNode, double fallbackValue) {
        final int node = (int)longNode; // XXX
        return (sourceIdMap.containsKey(node)) ?
                new ArrowAdjacencyCursor(sourceIdMap.get(node), Double.NaN)
                : AdjacencyCursor.EmptyAdjacencyCursor.INSTANCE;
    }

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
