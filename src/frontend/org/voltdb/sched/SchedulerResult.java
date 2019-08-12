/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sched;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Result returned by {@link Scheduler#nextRun(ScheduledProcedure)} to indicate the next step of the schedules life
 * cycle.
 */
public final class SchedulerResult {
    private final Type m_status;
    private final String m_message;
    private final ScheduledProcedure m_scheduledProcedure;

    /**
     * Create a {@link SchedulerResult} which indicates that an unrecoverable error has occurred and the scheduler must
     * exit.
     *
     * @param message to log indicating the details of the error. May be {@code null}
     * @return A new {@link Type#ERROR} instance of {@link SchedulerResult}
     */
    public static SchedulerResult createError(String message) {
        return new SchedulerResult(Type.ERROR, message, null);
    }

    /**
     * Create a {@link SchedulerResult} which indicates that the scheduler has reached the end of its life cycle
     * gracefully
     *
     * @param message to log indicating the details of the error. May be {@code null}
     * @return A new {@link Type#EXIT} instance of {@link SchedulerResult}
     */
    public static SchedulerResult createExit(String message) {
        return new SchedulerResult(Type.EXIT, message, null);
    }

    /**
     * Schedule a procedure to be executed after a delay
     *
     * @param delay               time for the procedure to be executed
     * @param timeUnit            {#link TimeUnit} of {@code delay}
     * @param procedure           name of procedure to execute
     * @param procedureParameters to pass to procedure during execution
     * @return A new {@link Type#PROCEDURE} instance of {@link SchedulerResult}
     */
    public static SchedulerResult createScheduledProcedure(long delay, TimeUnit timeUnit, String procedure,
            Object... procedureParameters) {
        return new SchedulerResult(Type.PROCEDURE, null,
                new ScheduledProcedure(delay, timeUnit, Objects.requireNonNull(procedure), procedureParameters));
    }

    /**
     * Schedule the scheduler to be invoked again without a procedure being executed. This causes the
     * {@link Scheduler#nextRun(ScheduledProcedure)} to be called again after a delay. A {@link ScheduledProcedure} will
     * be associated with this call however it will have a {@code null} procedure but an {@code attachment} can be used.
     *
     * @param delay    time for the scheduler to be executed
     * @param timeUnit {#link TimeUnit} of {@code delay}
     * @return A new {@link Type#RERUN} instance of {@link SchedulerResult}
     */
    public static SchedulerResult createRerun(long delay, TimeUnit timeUnit) {
        return new SchedulerResult(Type.RERUN, null, new ScheduledProcedure(delay, timeUnit, null));
    }

    private SchedulerResult(Type status, String message, ScheduledProcedure scheduledProcedure) {
        m_status = status;
        m_message = message;
        m_scheduledProcedure = scheduledProcedure;
    }

    /**
     * @return The {@link Type} of this result
     */
    public Type getType() {
        return m_status;
    }

    /**
     * Use {@link #hasMessage()} to determine if there is a message attached to this result
     *
     * @return Optional message provided with a {@link Type#EXIT} or {@link Type#ERROR} result
     */
    public String getMessage() {
        return m_message;
    }

    /**
     * @return {@code true} if this result has a message
     */
    public boolean hasMessage() {
        return m_message != null;
    }

    /**
     * Get the next {@link ScheduledProcedure} to execute when this result is {@link Type#PROCEDURE} or
     * {@link Type#RERUN}
     *
     * @return The next {@link ScheduledProcedure} definition
     */
    public ScheduledProcedure getScheduledProcedure() {
        return m_scheduledProcedure;
    }

    /**
     * Enum used to describe the type of the {@link SchedulerResult}.
     */
    public enum Type {
        /** Schedule a procedure to be executed */
        PROCEDURE,
        /** Schedule the scheduler to be invoked again without a procedure being executed */
        RERUN,
        /** Unexpected error occurred within the scheduler and another procedures will not be scheduled */
        ERROR,
        /** Scheduler has reached an end to its life cycle and is not scheduling any more procedures */
        EXIT;
    }
}
