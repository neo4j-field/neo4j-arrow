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
import org.neo4j.arrow.job.WriteJob;
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
    public static final String GDS_READ_ACTION = "gds.read";
    public static final String NODE_WRITE_ACTION = "gds.write.nodes";
    public static final String RELS_WRITE_ACTION = "gds.write.relationships";

    private static final List<String> supportedActions = List.of(GDS_READ_ACTION, RELS_WRITE_ACTION, NODE_WRITE_ACTION);
    private final Log log;
    private final JobCreator<Message, Job> jobCreator;

    public GdsActionHandler(JobCreator<Message, Job> jobCreator, Log log) {
        this.jobCreator = jobCreator;
        this.log = log;
    }

    @Override
    public List<String> actionTypes() {
        return supportedActions;
    }

    @Override
    public List<ActionType> actionDescriptions() {
        return List.of(new ActionType(GDS_READ_ACTION, "Stream node or relationship properties from a GDS Graph"),
                new ActionType(RELS_WRITE_ACTION, "Write relationship properties to a GDS Graph"),
                new ActionType(NODE_WRITE_ACTION, "Write Nodes and properties to a GDS Graph"));
    }

    @Override
    public Outcome handle(FlightProducer.CallContext context, Action action, Producer producer) {
        // XXX: assumption is we've set the peer identity to the username...
        // XXX: see org.neo4j.arrow.auth.NativeAuthValidator for details.

        final String username = context.peerIdentity();
        log.info("user '%s' attempting a GDS action: %s", username, action.getType());

        Message msg;

        switch (action.getType()) {
            case GDS_READ_ACTION:
                try {
                    msg = GdsMessage.deserialize(action.getBody());
                } catch (IOException e) {
                    return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("invalid gds message"));
                }
                return handleGdsReadAction(producer, username, (GdsMessage) msg);
            case NODE_WRITE_ACTION:
                try {
                    msg = GdsWriteNodeMessage.deserialize(action.getBody());
                } catch (IOException e) {
                    return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("invalid gds message"));
                }
                return handleGdsWriteAction(producer, username, msg);
            case RELS_WRITE_ACTION:
                try {
                    msg = GdsWriteRelsMessage.deserialize(action.getBody());
                } catch (IOException e) {
                    return Outcome.failure(CallStatus.INVALID_ARGUMENT.withDescription("invalid gds message"));
                }
                return handleGdsWriteAction(producer, username, msg);
            default:
                // fallthrough
        }
        return Outcome.failure(CallStatus.UNIMPLEMENTED.withDescription("coming soon?!"));
    }

    private Outcome handleGdsWriteAction(Producer producer, String username, Message msg) {
        final Job j = jobCreator.newJob(msg, Job.Mode.WRITE, username);
        assert(j instanceof WriteJob); // XXX
        final WriteJob job = (WriteJob)j;
        final Ticket ticket = producer.ticketJob(job);

        return Outcome.success(new Result(ticket.serialize().array()));
    }

    private Outcome handleGdsReadAction(Producer producer, String username, GdsMessage msg) {
        log.info("handling gds read action from username %s", username);
        try {
            final Job j = jobCreator.newJob(msg, Job.Mode.READ, username);
            assert (j instanceof ReadJob); // XXX
            final ReadJob job = (ReadJob) j;
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
                } finally {
                    log.info("got possible first record");
                }
                return Optional.empty();
            }).thenAcceptAsync(maybeRecord -> {
                if (maybeRecord.isEmpty()) {
                    // XXX: need handling of this problem :-(
                    log.warn("first record looks bogus");
                    producer.deleteFlight(ticket);
                    return;
                }
                try {
                    final RowBasedRecord record = (RowBasedRecord) maybeRecord.get();
                    final List<Field> fields = getSchemaFields(record);

                    log.info("publishing flight info for ticket %s", ticket);

                    // We've got our Schema, so publish this Flight for consumption
                    producer.setFlightInfo(ticket, new Schema(fields));
                } catch (Exception e) {
                    log.error("oops", e);
                }
            });

            // We're taking off, so hand the ticket back to our client.
            return Outcome.success(new Result(ticket.serialize().array()));
        } catch (Exception e) {
            log.error("handleGdsReadAction failed", e);
            return Outcome.failure(CallStatus.INTERNAL.withDescription(e.getMessage()));
        }
    }

    /**
     * Given an Arrow {@link RowBasedRecord}, generate a list of Arrow {@link Field}s
     * representing the schema of the stream.
     * @param record a {@link RowBasedRecord} with sample data
     * @return {@link List} of {@link Field}s
     */
    private List<Field> getSchemaFields(RowBasedRecord record) {
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
                case STRING_LIST:
                    fields.add(new Field(fieldName, FieldType.nullable(new ArrowType.List()),
                            List.of(new Field(fieldName,
                                    FieldType.nullable(new ArrowType.Utf8()),
                                    null))));
                    break;
                default:
                    // TODO: fallback to raw bytes?
                    log.error("unsupported value type for handler: {}", value.type());
            }
        });
        return fields;
    }
}
