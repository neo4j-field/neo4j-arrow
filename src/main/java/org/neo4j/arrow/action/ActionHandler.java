package org.neo4j.arrow.action;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.FlightProducer;
import org.neo4j.arrow.Producer;

import java.util.List;

public interface ActionHandler {
    /* List of supported action types this handler can process. */
    List<String> actionTypes();

    /* List fully formed Arrow Flight ActionTypes with descriptions. */
    List<ActionType> actionDescriptions();

    /* Primary action handler */
    Outcome handle(FlightProducer.CallContext context, Action action, Producer producer);
}
