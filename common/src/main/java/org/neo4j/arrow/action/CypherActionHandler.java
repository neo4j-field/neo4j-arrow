package org.neo4j.arrow.action;

import org.apache.arrow.flight.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.RowBasedRecord;
import org.neo4j.arrow.job.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CypherActionHandler implements ActionHandler {
    public static final String CYPHER_READ_ACTION = "cypherRead";
    public static final String CYPHER_WRITE_ACTION = "cypherWrite";

    private static final List<String> supportedActions = List.of(CYPHER_READ_ACTION, CYPHER_WRITE_ACTION);
    private static Logger logger = LoggerFactory.getLogger(CypherActionHandler.class);

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
                /* Ticket this job */
                final Job job = jobCreator.newJob(msg, Job.Mode.READ,
                        // TODO: get from context?
                        Optional.of("neo4j"), Optional.of("password"));
                final Ticket ticket = producer.ticketJob(job);

                /* We need to wait for the first record to discern our final schema */
                final Future<RowBasedRecord> futureRecord = job.getFirstRecord();

                CompletableFuture.supplyAsync(() -> {
                    try {
                        return Optional.of(futureRecord.get());
                    } catch (InterruptedException e) {
                        logger.error("interrupted getting first record", e);
                    } catch (ExecutionException e) {
                        logger.error("execution error", e);
                    }
                    return Optional.empty();
                }).thenAcceptAsync(maybeRecord -> {
                    if (maybeRecord.isEmpty()) {
                        // XXX: need handling of this problem :-(
                        producer.deleteFlight(ticket);
                        return;
                    }
                    final RowBasedRecord record = (RowBasedRecord) maybeRecord.get();

                    final List<Field> fields = new ArrayList<>();
                    record.keys().stream().forEach(fieldName -> {
                        final RowBasedRecord.Value value = record.get(fieldName);
                        final int size = value.size();
                        // TODO: better mapping support?
                        System.out.printf("xxx Translating Neo4j value %s -> %s\n", fieldName, value.type());

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
                                // Variable width List...suboptimal, but all we can do with Cypher :-(
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.List()), null));
                                break;
                            default:
                                // TODO: fallback to raw bytes?
                                logger.error("unsupported value type for handler: {}", value.type());
                        }
                    });
                    producer.setFlightInfo(ticket, new Schema(fields));
                });

                /* We're taking off, so hand the ticket back to our client. */
                return Outcome.success(new Result(ticket.serialize().array()));
            case CYPHER_WRITE_ACTION:
                return Outcome.failure(CallStatus.UNIMPLEMENTED.withDescription("can't do writes yet!"));
            default:
                logger.warn("unknown action {}", action.getType());
                return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("unknown action!"));
        }

    }
}
