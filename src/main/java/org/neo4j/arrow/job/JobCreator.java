package org.neo4j.arrow.job;

import org.apache.arrow.flight.Action;

import java.util.Optional;

@FunctionalInterface
public interface JobCreator<T> {
    Job newJob(T msg, Job.Mode mode, Optional<String> username, Optional<String> password);
}
