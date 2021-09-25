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
import org.neo4j.arrow.job.ReadJob;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Native integration with GDS via Arrow.
 * <p>
 * Provides jobs/services for reading properties from a graph projection in the Graph Catalog.
 */
public class GdsActionHandler implements ActionHandler {
    // TODO: rename property keys to read/write forms
    public static final String NODE_READ_ACTION = "gdsNodeProperties";
    public static final String RELS_READ_ACTION = "gdsRelProperties";
    public static final String NODE_WRITE_ACTION = "gds.write.nodes";
    public static final String RELS_WRITE_ACTION = "gds.write.relationships";

    private static final List<String> supportedActions = List.of(NODE_READ_ACTION, RELS_READ_ACTION);
    private final Log log;
    private final JobCreator<GdsMessage, ReadJob> jobCreator;

    public GdsActionHandler(JobCreator<GdsMessage, ReadJob> jobCreator, Log log) {
        this.jobCreator = jobCreator;
        this.log = log;
    }

    @Override
    public List<String> actionTypes() {
        return supportedActions;
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(NODE_READ_ACTION, "Stream node properties from a GDS Graph"),
                new ActionType(RELS_READ_ACTION, "Stream relationship properties from a GDS Graph"));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        // XXX: assumption is we've set the peer identity to the username...
        // XXX: see org.neo4j.arrow.auth.NativeAuthValidator for details.

        final String username = context.peerIdentity();
        log.info("user '%s' attempting a GDS action: %s", username, action.getType());
        GdsMessage msg;
        try {
            msg = GdsMessage.deserialize(action.getBody());
        } catch (IOException e) {
            return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("invalid gds message"));
        }

        switch (action.getType()) {
            case NODE_READ_ACTION:
                final ReadJob job = jobCreator.newJob(msg, Job.Mode.READ, username);
                final Ticket ticket = producer.ticketJob(job);

                // We need to wait for the first record to discern our final schema
                final Future<RowBasedRecord> futureRecord = job.getFirstRecord();

                CompletableFuture.supplyAsync(() -> {
                    // Try to get our first record
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

                    // Build the Arrow schema from our first record, assuming it's constant
                    final List<Field> fields = new ArrayList<>();
                    record.keys().forEach(fieldName -> {
                        final RowBasedRecord.Value value = record.get(fieldName);
                        log.info("Translating Neo4j value %s -> %s", fieldName, value.type());

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

                    // We've got our Schema, so publish this Flight for consumption
                    producer.setFlightInfo(ticket, new Schema(fields));
                });

                // We're taking off, so hand the ticket back to our client.
                return Outcome.success(new Result(ticket.serialize().array()));
            case NODE_WRITE_ACTION:

            case RELS_READ_ACTION:
            case RELS_WRITE_ACTION:
                // not implemented yet
                break;
        }
        return Outcome.failure(CallStatus.UNIMPLEMENTED.withDescription("coming soon?!"));
    }
}
