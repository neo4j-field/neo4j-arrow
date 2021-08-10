package org.neo4j.arrow.job;

import java.util.Optional;

@FunctionalInterface
public interface JobCreator {
    Job newJob(CypherMessage message, Job.Mode mode,
               Optional<String> username, Optional<String> password);
}
