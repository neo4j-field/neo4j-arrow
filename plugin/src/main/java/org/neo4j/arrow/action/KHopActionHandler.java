package org.neo4j.arrow.action;

import org.apache.arrow.flight.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.SubGraphRecord;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.JobCreator;
import org.neo4j.arrow.job.KHopJob;
import org.neo4j.arrow.job.ReadJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class KHopActionHandler implements ActionHandler {

    protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KHopActionHandler.class);

    public final static String KHOP_ACTION = "khop";
    private final JobCreator<KHopMessage, KHopJob> jobCreator;

    public KHopActionHandler(JobCreator<KHopMessage, KHopJob> jobCreator) {
        this.jobCreator = jobCreator;
    }

    @Override
    public List<String> actionTypes() {
        return List.of(KHOP_ACTION);
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(KHOP_ACTION, "experimental k-hop implementation"));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        logger.info("handling possible khop action");

        try {
            final String username = context.peerIdentity();
            logger.info("user '{}' attempting a Khop action: {}", username, action.getType());

            final KHopMessage msg = KHopMessage.deserialize(action.getBody());
            final ReadJob job = jobCreator.newJob(msg, Job.Mode.READ, username);
            final Ticket ticket = producer.ticketJob(job);

            final Future<RowBasedRecord> futureRecord = job.getFirstRecord();

            CompletableFuture.runAsync(() -> {
                // TODO: schema stuff?

                try {
                    futureRecord.get(60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.info(e.getMessage(), e);
                    // XXX
                }
                final List<Field> fields = new ArrayList<>();
                // XXX 64 bit signed vs 64 bit unsigned issue here
                fields.add(new Field(SubGraphRecord.KEY_ORIGIN_ID, FieldType.nullable(new ArrowType.Int(64, true)), null));

                fields.add(new Field(SubGraphRecord.KEY_SOURCE_ID, FieldType.nullable(new ArrowType.Int(64, true)), null));
                fields.add(new Field(SubGraphRecord.KEY_SOURCE_LABELS, FieldType.nullable(new ArrowType.List()),
                        List.of(new Field(SubGraphRecord.KEY_SOURCE_LABELS, FieldType.nullable(new ArrowType.Utf8()), null))));

                fields.add(new Field(SubGraphRecord.KEY_REL_TYPE, FieldType.nullable(new ArrowType.Utf8()), null));

                fields.add(new Field(SubGraphRecord.KEY_TARGET_ID, FieldType.nullable(new ArrowType.Int(64, true)), null));
                fields.add(new Field(SubGraphRecord.KEY_TARGET_LABELS, FieldType.nullable(new ArrowType.List()),
                        List.of(new Field(SubGraphRecord.KEY_TARGET_LABELS, FieldType.nullable(new ArrowType.Utf8()), null))));
                producer.setFlightInfo(ticket, new Schema(fields));
            });

            return Outcome.success(new Result(ticket.serialize().array()));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription(e.getMessage()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return Outcome.failure(CallStatus.UNKNOWN.withDescription(e.getMessage()));
        }
    }
}
