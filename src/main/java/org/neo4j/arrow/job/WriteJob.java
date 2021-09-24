package org.neo4j.arrow.job;

public class WriteJob extends Job {
    public static final Mode mode = Mode.WRITE;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public void close() throws Exception {

    }
}
