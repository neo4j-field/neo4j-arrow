package org.neo4j.arrow.job;

import org.neo4j.driver.summary.ResultSummary;

public class DriverJobSummary implements JobSummary {
    private final String resultSummary;
    private DriverJobSummary(ResultSummary summary) {
        this.resultSummary = summary.toString();
    }

    public static JobSummary wrap(ResultSummary summary) {
        return summary::toString;
    }

    @Override
    public String asString() {
        return resultSummary;
    }
}
