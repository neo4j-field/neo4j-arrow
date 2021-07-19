package org.neo4j.arrow;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.io.Closeable;

public class Neo4jJob implements Closeable {

    private Session session;
    private Result result;

    public Neo4jJob(Session session, Result result) {
        this.session = session;
        this.result = result;
    }

    public Result getResult() {
        return result;
    }

    @Override
    public void close() {
        session.close();
    }
}
