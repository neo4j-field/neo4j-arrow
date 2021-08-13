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
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class GdsActionhandler implements ActionHandler {
    public static final String NODE_PROPS_ACTION = "gdsNodeProperties";
    public static final String REL_PROPS_ACTION = "gdsRelProperties";

    private static final List<String> supportedActions = List.of(NODE_PROPS_ACTION, REL_PROPS_ACTION);
    private final Log log;
    private final JobCreator jobCreator;

    public GdsActionhandler(JobCreator<GdsMessage> jobCreator, Log log) {
        this.jobCreator = jobCreator;
        this.log = log;
    }

    @Override
    public List<String> actionTypes() {
        return supportedActions;
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(NODE_PROPS_ACTION, "Stream node properties from a GDS Graph"),
                new ActionType(REL_PROPS_ACTION, "Stream relationship properties from a GDS Graph"));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        // XXX: assumption is we've set the peer identity to the user.
        final String username = context.peerIdentity();
        GdsMessage msg;
        try {
            msg = GdsMessage.deserialize(action.getBody());
        } catch (IOException e) {
            e.printStackTrace();
            return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("invalid gds message"));
        }

        switch (action.getType()) {
            case NODE_PROPS_ACTION:
                Job job = jobCreator.newJob(msg, Job.Mode.READ,
                        Optional.of(username), Optional.empty());
                final Ticket ticket = producer.ticketJob(job);
                /* We need to wait for the first record to discern our final schema */
                final Future<RowBasedRecord> futureRecord = job.getFirstRecord();

                CompletableFuture.supplyAsync(() -> {
                    try {
                        return Optional.of(futureRecord.get());
                    } catch (InterruptedException e) {
                        log.error("interrupted getting first record", e);
                    } catch (ExecutionException e) {
                        log.error("execution error", e);
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
                            case INT_ARRAY:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FixedSizeList(value.size())),
                                        List.of(new Field(fieldName,
                                                FieldType.nullable(new ArrowType.Int(32, true)),
                                                null))));
                                break;
                            case LONG_ARRAY:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FixedSizeList(value.size())),
                                        List.of(new Field(fieldName,
                                                FieldType.nullable(new ArrowType.Int(64, true)),
                                                null))));
                                break;
                            case FLOAT_ARRAY:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FixedSizeList(value.size())),
                                        List.of(new Field(fieldName,
                                                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                                                null))));
                                break;
                            case DOUBLE_ARRAY:
                                fields.add(new Field(fieldName,
                                        FieldType.nullable(new ArrowType.FixedSizeList(value.size())),
                                        List.of(new Field(fieldName,
                                                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                                                null))));
                                break;
                            default:
                                // TODO: fallback to raw bytes?
                                log.error("unsupported value type for handler: {}", value.type());
                        }
                    });
                    producer.setFlightInfo(ticket, new Schema(fields));
                });

                /* We're taking off, so hand the ticket back to our client. */
                return Outcome.success(new Result(ticket.serialize().array()));
            case REL_PROPS_ACTION:
                // not implemented
                break;
        }
        return Outcome.failure(CallStatus.UNIMPLEMENTED.withDescription("coming soon!"));
    }
}
