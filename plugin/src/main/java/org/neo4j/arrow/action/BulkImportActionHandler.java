package org.neo4j.arrow.action;

import org.apache.arrow.flight.*;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.job.BulkImportJob;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.JobCreator;

import java.io.IOException;
import java.util.List;

public class BulkImportActionHandler implements ActionHandler {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BulkImportActionHandler.class);

    private final JobCreator<BulkImportMessage, BulkImportJob> jobCreator;
    public static String importBulkActionType = "import.bulk";
    public static String importBulkActionDescription = "Use neo4j bulk import to bootstrap a new database.";

    public BulkImportActionHandler(JobCreator<BulkImportMessage, BulkImportJob> jobCreator) {
        this.jobCreator = jobCreator;
    }

    @Override
    public List<String> actionTypes() {
        return List.of(importBulkActionType);
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(importBulkActionType, importBulkActionDescription));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        final String username = context.peerIdentity();
        logger.info("user {} attempting a bulk import action", username);

        // XXX assert user is admin/neo4j?

        try {
            final BulkImportMessage msg = BulkImportMessage.deserialize(action.getBody());
            final BulkImportJob job = jobCreator.newJob(msg, Job.Mode.WRITE, username);
            final Ticket ticket = producer.ticketJob(job);
            return Outcome.success(new Result(ticket.serialize().array()));

        } catch (IOException e) {
            e.printStackTrace();
            return Outcome.failure(CallStatus.INTERNAL.withDescription(e.getMessage()));
        }
    }
}
