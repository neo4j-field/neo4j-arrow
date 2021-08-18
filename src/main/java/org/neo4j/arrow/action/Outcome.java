package org.neo4j.arrow.action;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Result;

import java.util.Optional;

/**
 * The outcome of processing an Arrow Flight RPC call.
 * <p>
 *     Effectively a pair of {@link Optional} results: a {@link Result} on success and a
 *     {@link CallStatus} on failure.
 * </p>
 */
public class Outcome {
    public final Optional<Result> result;
    public final Optional<CallStatus> callStatus;

    protected Outcome(Result result, CallStatus callStatus) {
        this.result = Optional.ofNullable(result);
        this.callStatus = Optional.ofNullable(callStatus);

        assert !(this.result.isPresent() && this.callStatus.isPresent());
        assert !(this.result.isEmpty() && this.callStatus.isEmpty());
    }

    /**
     * Creates a new failure {@link Outcome} from the provided {@link CallStatus}.
     *
     * @param callStatus {@link CallStatus} representing the Arrow Flight RPC failure event
     * @return a new {@link Outcome}
     */
    public static Outcome failure(CallStatus callStatus) {
        return new Outcome(null, callStatus);
    }

    /**
     * Creates a new successful {@Outcome} from the provided Arrow Flight RPC {@link Result}.
     *
     * @param result the successful {@link Result} to return
     * @return a new {@link Outcome}
     */
    public static Outcome success(Result result) {
        return new Outcome(result, null);
    }

    /**
     * Returns whether the {@link Outcome} is considered successful or not.
     *
     * @return true if successful, otherwise false.
     */
    public boolean isSuccessful() {
        return result.isPresent();
    }
}
