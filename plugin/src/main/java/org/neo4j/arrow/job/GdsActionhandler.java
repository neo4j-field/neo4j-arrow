package org.neo4j.arrow.job;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.neo4j.arrow.Producer;
import org.neo4j.arrow.action.ActionHandler;
import org.neo4j.arrow.action.Outcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GdsActionhandler implements ActionHandler {
    public static final String NODE_PROPS_ACTION = "getNodeProperties";
    public static final String REL_PROPS_ACTION = "getRelProperties";

    private static final List<String> supportedActions = List.of(NODE_PROPS_ACTION, REL_PROPS_ACTION);
    private static Logger logger = LoggerFactory.getLogger(GdsActionhandler.class);

    private final JobCreator jobCreator;

    public GdsActionhandler(JobCreator jobCreator) {
        this.jobCreator = jobCreator;
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
        return Outcome.failure(CallStatus.UNIMPLEMENTED.withDescription("coming soon!"));
    }
}
