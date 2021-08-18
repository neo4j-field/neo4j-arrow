package org.neo4j.arrow.action;

import org.apache.arrow.flight.*;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Report on the status of active, completed, or pending Jobs
 */
public class StatusHandler implements ActionHandler {
    private static Logger logger = LoggerFactory.getLogger(StatusHandler.class);

    public static final String STATUS_ACTION = "jobStatus";

    @Override
    public List<String> actionTypes() {
        return List.of(STATUS_ACTION);
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(STATUS_ACTION, "Check the status of a Job"));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        // TODO: standardize on matching logic? case sensitive/insensitive?
        if (!action.getType().equalsIgnoreCase(STATUS_ACTION)) {
            return Outcome.failure(CallStatus.UNKNOWN.withDescription("Unsupported action for job status handler"));
        }

        try {
            final Ticket ticket = Ticket.deserialize(ByteBuffer.wrap(action.getBody()));
            Job job = producer.getJob(ticket);
            if (job != null) {
                return Outcome.success(new Result(job.getStatus().toString().getBytes(StandardCharsets.UTF_8)));
            }
            return Outcome.failure(CallStatus.NOT_FOUND.withDescription("no job for ticket"));
        } catch (IOException e) {
            logger.error("Problem servicing cypher status action", e);
            return Outcome.failure(CallStatus.INTERNAL.withDescription(e.getMessage()));
        }
    }
}
