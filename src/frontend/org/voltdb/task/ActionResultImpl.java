/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.task;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.ClientResponse;

final class ActionResultImpl implements ActionResult {
    private final ScheduledAction m_action;
    private ClientResponse m_response;

    private long m_expectedExecutionTimeNs;
    private long m_startedAtNs;
    private long m_completedAtNs;

    ActionResultImpl(ScheduledAction action) {
        m_action = action;
    }

    @Override
    public ActionType getType() {
        return m_action.getType();
    }

    @Override
    public long getInterval(TimeUnit unit) {
        return m_action.getInterval(unit);
    }

    @Override
    public String getProcedure() {
        return m_action.getProcedure();
    }

    @Override
    public Object[] getProcedureParameters() {
        return m_action.getProcedureParameters();
    }

    Object[] getRawProcedureParameters() {
        return m_action.getRawProcedureParameters();
    }

    @Override
    public ClientResponse getResponse() {
        return m_response;
    }

    @Override
    public <T> T getAttachment() throws ClassCastException {
        return m_action.getAttachment();
    }

    ScheduledAction callCallback() {
        return m_action.getCallback().apply(this);
    }

    ActionResultImpl setResponse(ClientResponse response) {
        m_response = Objects.requireNonNull(response);
        m_completedAtNs = System.nanoTime();
        return this;
    }

    ActionResultImpl setExpectedExecutionTime(long expectedExecutionTimeNs) {
        m_expectedExecutionTimeNs = expectedExecutionTimeNs;
        return this;
    }

    ActionResultImpl setStarted() {
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
