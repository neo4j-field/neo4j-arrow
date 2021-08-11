package org.neo4j.arrow.action;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Result;

import java.util.Optional;

public class Outcome {
    public final Optional<Result> result;
    public final Optional<CallStatus> callStatus;

    public Outcome(Result result, CallStatus callStatus) {
        this.result = Optional.ofNullable(result);
        this.callStatus = Optional.ofNullable(callStatus);
    }

    public static Outcome failure(CallStatus callStatus) {
        return new Outcome(null, callStatus);
    }

    public static Outcome success(Result result) {
        return new Outcome(result, null);
    }

    public boolean isSuccessful() {
        return result.isPresent();
    }
}
