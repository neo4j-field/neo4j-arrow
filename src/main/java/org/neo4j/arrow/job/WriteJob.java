package org.neo4j.arrow.job;


import java.util.function.BiConsumer;

public abstract class WriteJob extends Job {

    public static final Mode mode = Mode.WRITE;

    private BiConsumer<Long, String[]> consumer;

    public WriteJob() {
        super();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public void close() throws Exception {

    }

    public void setConsumer(BiConsumer<Long, String[]> consumer) {
        this.consumer = consumer;
    }

    public BiConsumer<Long, String[]> getConsumer() {
        return consumer;
    }
}
