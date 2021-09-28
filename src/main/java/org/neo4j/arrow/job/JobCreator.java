package org.neo4j.arrow.job;

import javax.annotation.Nullable;

/**
 * The {@link JobCreator} provides a functional interface for creating an instance of a {@link Job}.
 * <p>
 *     Since each {@link Job} implementation potentially uses a distinct message format (and there's
 *     currently no message interface or base class), the {@link JobCreator} is generic and
 *     parameterized by the type T of the supported message.
 * </p>
 * <p>
 *     It's assumed that things like the {@link Job.Mode}, a username, and password are common
 *     enough to warrant being in the core signature. (Albeit username and password are optional.)
 * </p>
 */
@FunctionalInterface
public interface JobCreator<T, J extends Job> {
    /**
     * Create a new {@link Job} given the job message, {@link Job.Mode}, and optional username and
     * password.
     *
     * @param msg a {@link Job}-specific message
     * @param mode the mode, e.g. READ vs WRITE
     * @param username optional username for the caller
     * @return new {@link Job}
     */
    J newJob(T msg, Job.Mode mode, @Nullable String username);
}
