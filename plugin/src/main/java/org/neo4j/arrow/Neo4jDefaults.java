package org.neo4j.arrow;

/**
 * Global defaults to keep consistency across Neo4j-specific Messages.
 */
public class Neo4jDefaults {
    /** Used for Node ids. */
    public static final String ID_FIELD = "ID";

    /** Used for Node labels. */
    public static final String LABELS_FIELD = "LABELS";

    /** Beginning Node id in a Relationship. */
    public static final String SOURCE_FIELD = "START_ID";

    /** Ending Node id in a Relationship. */
    public static final String TARGET_FIELD = "END_ID";

    /** Relationship Type. */
    public static final String TYPE_FIELD = "TYPE";
}
