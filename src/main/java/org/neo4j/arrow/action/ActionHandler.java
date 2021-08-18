package org.neo4j.arrow.action;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.FlightProducer;
import org.neo4j.arrow.Producer;

import java.util.List;

/**
 * An {@link ActionHandler} provides handling for different Arrow Flight {@link Action} types.
 * <p>
 *     When registered with a {@link Producer}, any {@link Action} that matches a type in an
 *     instances {@link #actionTypes()} will be passed to the given {@link ActionHandler} via
 *     its {@link #handle(FlightProducer.CallContext, Action, Producer)} method.
 * </p>
 */
public interface ActionHandler {
    /**
     * Get the list of action types supported by this handler.
     *
     * @return a {@link List} of supported {@link Action} types as {@link String}s
     */
    List<String> actionTypes();

    /**
     * Get a list of descriptions (as {@link ActionType} instances) supported by this handler.
     *
     * @return a {@link List} of {@ActionType}s supported
     */
    List<ActionType> actionDescriptions();

    /**
     * Handle an Arrow Flight RPC {@link Action}.
     *
     * @param context a reference to the caller's {@link org.apache.arrow.flight.FlightProducer.CallContext}
     *                providing access to the peer identity of the caller
     * @param action the {@link Action} to process
     * @param producer reference to the controlling {@link Producer}
     * @return
     */
    Outcome handle(FlightProducer.CallContext context, Action action, Producer producer);
}
