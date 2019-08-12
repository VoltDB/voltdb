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

import org.voltdb.client.ClientResponse;

/**
 * Defines a procedure scheduled for execution by a {@link Scheduler} instance and the result of the execution.
 */
public final class ScheduledProcedure {
    private final long m_requestedDelayNs;
    private final String m_procedure;
    private final Object[] m_procedureParameters;
    private ClientResponse m_response;
    private Object m_attachment;

    long m_expectedExecutionTimeNs;
    long m_startedAtNs;
    long m_completedAtNs;

    ScheduledProcedure(long delay, TimeUnit timeUnit, String procedure, Object... procedureParameters) {
        m_requestedDelayNs = Math.max(timeUnit.toNanos(delay), 0);
        m_procedure = procedure;
        m_procedureParameters = procedureParameters.clone();
    }

    /**
     * @param timeUnit {@link TimeUnit} of delay returned by this method
     * @return Time delay in {@code timeUnit}
     */
    public long getDelay(TimeUnit timeUnit) {
        return timeUnit.convert(m_requestedDelayNs, TimeUnit.NANOSECONDS);
    }

    /**
     * @return Name of procedure to execute. May be {@code null} if this is a forced rerun of the {@link Scheduler}
     */
    public String getProcedure() {
        return m_procedure;
    }

    /**
     * @return A The parameters that are to be passed the the procedure returned by {@link #getProcedure()}
     */
    public Object[] getProcedureParameters() {
        return m_procedureParameters.clone();
    }

    Object[] getRawProcedureParameters() {
        return m_procedureParameters;
    }

    /**
     * Add an arbitrary attachment to this instance so that it can be retrieved upon the next {@link Scheduler}
     * invocation.
     *
     * @param attachment object to attach. May be {@code null}
     * @return {@code this}
     */
    public ScheduledProcedure setAttachment(Object attachment) {
        m_attachment = attachment;
        return this;
    }

    /**
     * Retrieve the attachment associated with this scheduled procedure
     *
     * @param <T> Type of attachment
     * @return The attachement or {@code null} of there was no attachment
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttachment() {
        return (T) m_attachment;
    }

    /**
     * @return {@link ClientResponse} from the execution of {@link #getProcedure()}. Will be null if procedure has not
     *         be executed or this is a rerun of the scheduler
     */
    public ClientResponse getResponse() {
        return m_response;
    }

    ScheduledProcedure setResponse(ClientResponse response) {
        m_response = Objects.requireNonNull(response);
        m_completedAtNs = System.nanoTime();
        return this;
    }

    ScheduledProcedure expectedExecutionTime(long expectedExecutionTimeNs) {
        m_expectedExecutionTimeNs = expectedExecutionTimeNs;
        return this;
    }

    ScheduledProcedure setStarted() {
        m_startedAtNs = System.nanoTime();
        return this;
    }

    long getExecutionTime() {
        return m_completedAtNs - m_startedAtNs;
    }

    long getWaitTime() {
        return m_startedAtNs - m_expectedExecutionTimeNs;
    }
}
