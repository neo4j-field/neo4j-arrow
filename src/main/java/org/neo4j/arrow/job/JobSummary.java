package org.neo4j.arrow.job;

/**
 * This is pretty much useless at the moment...probably will be killed.
 */
@FunctionalInterface
public interface JobSummary {
    @SuppressWarnings("unused")
    String asString();
}
