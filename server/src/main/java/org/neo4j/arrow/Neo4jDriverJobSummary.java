package org.neo4j.arrow;

import org.neo4j.driver.summary.ResultSummary;

public class Neo4jDriverJobSummary implements JobSummary {
    public static JobSummary wrap(ResultSummary summary) {
        return new JobSummary() {
            @Override
            public String toString() {
                return summary.toString();
            }
        };
    }
}
