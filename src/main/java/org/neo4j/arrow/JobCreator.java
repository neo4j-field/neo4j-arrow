package org.neo4j.arrow;

import java.util.Optional;

public interface JobCreator {
    Neo4jJob newJob(CypherMessage message, Neo4jJob.Mode mode,
                    Optional<String> username, Optional<String> password);
}
