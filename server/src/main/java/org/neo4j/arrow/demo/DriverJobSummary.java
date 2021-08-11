package org.neo4j.arrow.demo;

import org.neo4j.arrow.job.JobSummary;
import org.neo4j.driver.summary.ResultSummary;

public class DriverJobSummary implements JobSummary {
    public static JobSummary wrap(ResultSummary summary) {
        return () ->  summary.toString();
    }

    @Override
    public String asString() {
        return null;
    }
}
