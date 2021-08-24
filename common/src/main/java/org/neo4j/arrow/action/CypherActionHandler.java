package org.neo4j.arrow.action;

import org.apache.arrow.flight.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.job.Job;
import org.neo4j.arrow.job.JobCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Orchestrate Cypher read and write-based {@link Job}s. (The exact mechanism of submitting the Cypher
 * is depending on the type of {@link Job} created by the {@link JobCreator}.)
 */
public class CypherActionHandler implements ActionHandler {

    /** A reading Cypher transaction */
    public static final String CYPHER_READ_ACTION = "cypherRead";
    /** A writing Cypher transaction */
    public static final String CYPHER_WRITE_ACTION = "cypherWrite";

    private static final List<String> supportedActions = List.of(CYPHER_READ_ACTION, CYPHER_WRITE_ACTION);
    private static Logger logger = LoggerFactory.getLogger(CypherActionHandler.class);

    /** A {@link JobCreator} that works with {@link CypherMessage}s */
    private final JobCreator<CypherMessage> jobCreator;

    public CypherActionHandler(JobCreator<CypherMessage> jobCreator) {
        this.jobCreator = jobCreator;
    }

    @Override
    public List<String> actionTypes() {
        return supportedActions;
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(CYPHER_READ_ACTION, "Submit a new Cypher-based read job"),
                new ActionType(CYPHER_WRITE_ACTION, "Submit a new Cypher-based write job"));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        CypherMessage msg;
        try {
            msg = CypherMessage.deserialize(action.getBody());
        } catch (IOException e) {
            return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("invalid CypherMessage"));
        }

        switch (action.getType()) {
            case CYPHER_READ_ACTION:
                final Job job = jobCreator.newJob(msg, Job.Mode.READ,
                        // TODO: get Cypher username/password from Context?
                        Optional.of("neo4j"), Optional.of("password"));
                final Ticket ticket = producer.ticketJob(job);

                /* We need to wait for the first record to discern our final schema */
                final Future<RowBasedRecord> futureRecord = job.getFirstRecord();

                CompletableFuture.supplyAsync(() -> {
                    try {
                        logger.info("waiting for first record for job {}", job.getJobId());
                        return Optional.of(futureRecord.get());
                    } catch (InterruptedException e) {
                        logger.error("Interrupted getting first record", e);
                    } catch (ExecutionException e) {
                        logger.error("Execution error getting first record", e);
                    }
                    return Optional.empty();
                }).thenAcceptAsync(maybeRecord -> {
                    if (maybeRecord.isEmpty()) {
                        // XXX: need handling of this problem :-(
                        logger.error("for some reason we didn't get a record for job {}", job.getJobId());
                        producer.deleteFlight(ticket);
                        return;
                    }
                    final RowBasedRecord record = (RowBasedRecord) maybeRecord.get();
                    logger.info("got first record for job {}", job.getJobId());

                    final List<Field> fields = new ArrayList<>();
                    record.keys().stream().forEach(fieldName -> {
                        final RowBasedRecord.Value value = record.get(fieldName);
                        // TODO: better mapping support for generic Cypher values?
                        logger.info("translating Neo4j value {} -> {}", fieldName, value.type());

                        switch (value.type()) {
                            case INT:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.Int(32, true)), null));
                                break;
                            case LONG:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.Int(64, true)), null));
                                break;
                            case FLOAT:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
                                break;
                            case DOUBLE:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                                break;
                            case STRING:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.Utf8()), null));
                                break;
                            case LIST:
                                // Variable width List...suboptimal, but all we can do with Cypher :-( Assume Doubles for now.
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.List()),
                                        List.of(new Field(fieldName,
                                                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                                                null))));
                                break;
                            default:
                                // TODO: fallback to raw bytes?
                                logger.error("unsupported value type for handler: {}", value.type());
                        }
                    });
                    producer.setFlightInfo(ticket, new Schema(fields));
                });

                // We're taking off, so hand the ticket back to our client.
                return Outcome.success(new Result(ticket.serialize().array()));

            case CYPHER_WRITE_ACTION:
                return Outcome.failure(CallStatus.UNIMPLEMENTED.withDescription("Can't do cypher writes yet!"));

            default:
                logger.warn("unknown action {}", action.getType());
                return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("Unknown action for handler"));
        }

    }
}
