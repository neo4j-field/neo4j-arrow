package org.neo4j.arrow.gds;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.*;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.storable.NumberType;

import java.time.ZonedDateTime;
import java.util.*;

/**
 * Mostly a hacky wrapper to help get hooks into graph destruction events so we
 * can manage memory.
 */
public class ArrowGraphStore implements GraphStore {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowGraphStore.class);

    private final GraphStore wrappedStore;
    private final Runnable onRelease;

    private ArrowGraphStore(GraphStore wrapped, Runnable onRelease) {
        this.wrappedStore = wrapped;
        this.onRelease = onRelease;
    }

    public static GraphStore wrap(GraphStore store, Runnable onRelease) {
        return new ArrowGraphStore(store, onRelease);
    }

    @Override
    public NamedDatabaseId databaseId() {
        return wrappedStore.databaseId();
    }

    @Override
    public GraphSchema schema() {
        return wrappedStore.schema();
    }

    @Override
    public ZonedDateTime modificationTime() {
        return wrappedStore.modificationTime();
    }

    @Override
    public long nodeCount() {
        return wrappedStore.nodeCount();
    }

    @Override
    public NodeMapping nodes() {
        return wrappedStore.nodes();
    }

    @Override
    public Set<NodeLabel> nodeLabels() {
        return wrappedStore.nodeLabels();
    }

    @Override
    public Set<String> nodePropertyKeys(NodeLabel label) {
        return wrappedStore.nodePropertyKeys(label);
    }

    @Override
    public Map<NodeLabel, Set<String>> nodePropertyKeys() {
        return wrappedStore.nodePropertyKeys();
    }

    @Override
    public boolean hasNodeProperty(NodeLabel label, String propertyKey) {
        return wrappedStore.hasNodeProperty(label, propertyKey);
    }

    @Override
    public boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey) {
        return wrappedStore.hasNodeProperty(labels, propertyKey);
    }

    @Override
    public Collection<String> nodePropertyKeys(Collection<NodeLabel> labels) {
        return wrappedStore.nodePropertyKeys(labels);
    }

    @Override
    public ValueType nodePropertyType(NodeLabel label, String propertyKey) {
        return wrappedStore.nodePropertyType(label, propertyKey);
    }

    @Override
    public PropertyState nodePropertyState(String propertyKey) {
        return wrappedStore.nodePropertyState(propertyKey);
    }

    @Override
    public NodeProperties nodePropertyValues(String propertyKey) {
        return wrappedStore.nodePropertyValues(propertyKey);
    }

    @Override
    public NodeProperties nodePropertyValues(NodeLabel label, String propertyKey) {
        return wrappedStore.nodePropertyValues(label, propertyKey);
    }

    @Override
    public void addNodeProperty(NodeLabel nodeLabel, String propertyKey, NodeProperties propertyValues) {
        wrappedStore.addNodeProperty(nodeLabel, propertyKey, propertyValues);
    }

    @Override
    public void removeNodeProperty(NodeLabel nodeLabel, String propertyKey) {
        wrappedStore.removeNodeProperty(nodeLabel, propertyKey);
    }

    @Override
    public long relationshipCount() {
        return wrappedStore.relationshipCount();
    }

    @Override
    public long relationshipCount(RelationshipType relationshipType) {
        return wrappedStore.relationshipCount(relationshipType);
    }

    @Override
    public Set<RelationshipType> relationshipTypes() {
        return wrappedStore.relationshipTypes();
    }

    @Override
    public boolean hasRelationshipType(RelationshipType relationshipType) {
        return wrappedStore.hasRelationshipType(relationshipType);
    }

    @Override
    public boolean hasRelationshipProperty(RelationshipType relType, String propertyKey) {
        return wrappedStore.hasRelationshipProperty(relType, propertyKey);
    }

    @Override
    public Collection<String> relationshipPropertyKeys(Collection<RelationshipType> relTypes) {
        return wrappedStore.relationshipPropertyKeys(relTypes);
    }

    @Override
    public ValueType relationshipPropertyType(String propertyKey) {
        return wrappedStore.relationshipPropertyType(propertyKey);
    }

    @Override
    public Set<String> relationshipPropertyKeys() {
        return wrappedStore.relationshipPropertyKeys();
    }

    @Override
    public Set<String> relationshipPropertyKeys(RelationshipType relationshipType) {
        return wrappedStore.relationshipPropertyKeys(relationshipType);
    }

    @Override
    public RelationshipProperty relationshipPropertyValues(RelationshipType relationshipType, String propertyKey) {
        return wrappedStore.relationshipPropertyValues(relationshipType, propertyKey);
    }

    @Override
    public void addRelationshipType(RelationshipType relationshipType, Optional<String> relationshipPropertyKey, Optional<NumberType> relationshipPropertyType, Relationships relationships) {
        wrappedStore.addRelationshipType(relationshipType, relationshipPropertyKey, relationshipPropertyType, relationships);
    }

    @Override
    public DeletionResult deleteRelationships(RelationshipType relationshipType) {
        return wrappedStore.deleteRelationships(relationshipType);
    }

    @Override
    public Graph getGraph(RelationshipType... relationshipType) {
        return wrappedStore.getGraph(relationshipType);
    }

    @Override
    public Graph getGraph(RelationshipType relationshipType, Optional<String> relationshipProperty) {
        return wrappedStore.getGraph(relationshipType, relationshipProperty);
    }

    @Override
    public Graph getGraph(Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        return wrappedStore.getGraph(relationshipTypes, maybeRelationshipProperty);
    }

    @Override
    public Graph getGraph(String nodeLabel, String relationshipType, Optional<String> maybeRelationshipProperty) {
        return wrappedStore.getGraph(nodeLabel, relationshipType, maybeRelationshipProperty);
    }

    @Override
    public Graph getGraph(NodeLabel nodeLabel, RelationshipType relationshipType, Optional<String> maybeRelationshipProperty) {
        return wrappedStore.getGraph(nodeLabel, relationshipType, maybeRelationshipProperty);
    }

    @Override
    public Graph getGraph(Collection<NodeLabel> nodeLabels, Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        return wrappedStore.getGraph(nodeLabels, relationshipTypes, maybeRelationshipProperty);
    }

    @Override
    public Graph getUnion() {
        return wrappedStore.getUnion();
    }

    @Override
    public CompositeRelationshipIterator getCompositeRelationshipIterator(RelationshipType relationshipType, List<String> propertyKeys) {
        return wrappedStore.getCompositeRelationshipIterator(relationshipType, propertyKeys);
    }

    @Override
    public void canRelease(boolean canRelease) {
        wrappedStore.canRelease(canRelease);
    }

    @Override
    public void release() {
        wrappedStore.release();
        logger.info("calling onRelease()");
        onRelease.run();
    }
}
