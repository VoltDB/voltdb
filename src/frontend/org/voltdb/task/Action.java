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

package org.voltdb.task;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Action to be performed on behalf of a {@link Scheduler} as returned by {@link Scheduler#getFirstAction()} or by
 * {@link Scheduler#getNextAction(ActionResult)}.
 */
public final class Action {
    private final Type m_type;
    private String m_statusMessage;
    private final ScheduledAction m_scheduledAction;

    /**
     * Create an {@link Action} which indicates that an unrecoverable error has occurred and the scheduler must exit.
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage to log indicating the details of the error. May be {@code null}
     * @return A new {@link Type#ERROR} instance of {@link Action}
     */
    public static Action createError(String statusMessage) {
        return new Action(Type.ERROR, statusMessage, null);
    }

    /**
     * Create an {@link Action} which indicates that the scheduler has reached the end of its life cycle gracefully
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage to log indicating the details of the error. May be {@code null}
     * @return A new {@link Type#EXIT} instance of {@link Action}
     */
    public static Action createExit(String statusMessage) {
        return new Action(Type.EXIT, statusMessage, null);
    }

    /**
     * Schedule a procedure to be executed after a delay
     *
     * @param delay               time for the procedure to be executed
     * @param timeUnit            {#link TimeUnit} of {@code delay}
     * @param procedure           name of procedure to execute
     * @param procedureParameters to pass to procedure during execution
     * @return A new {@link Type#PROCEDURE} instance of {@link Action}
     */
    public static Action createProcedure(long delay, TimeUnit timeUnit, String procedure,
            Object... procedureParameters) {
        return new Action(Type.PROCEDURE, null, new ScheduledAction(Type.PROCEDURE, delay, timeUnit,
                Objects.requireNonNull(procedure), procedureParameters));
    }

    /**
     * Schedule the scheduler to be invoked again without a procedure being executed. This causes the
     * {@link Scheduler#getNextAction(ActionResult)} to be called again after a delay. A {@link ActionResult} will be
     * associated with this call however it will have a {@code null} procedure but an {@code attachment} can be used.
     *
     * @param delay    time for the scheduler to be executed
     * @param timeUnit {@link TimeUnit} of {@code delay}
     * @return A new {@link Type#RERUN} instance of {@link Action}
     */
    public static Action createRerun(long delay, TimeUnit timeUnit) {
        return new Action(Type.RERUN, null, new ScheduledAction(Type.RERUN, delay, timeUnit, null));
    }

    private Action(Type status, String message, ScheduledAction scheduledAction) {
        m_type = status;
        m_statusMessage = message;
        m_scheduledAction = scheduledAction;
    }

    /**
     * @return The {@link Type} of this action
     */
    public Type getType() {
        return m_type;
    }

    /**
     * @return Optional status message provided with any action
     */
    public String getStatusMessage() {
        return m_statusMessage;
    }

    /**
     * Set the optional status massage which will be reported in the statistics for a {@link Scheduler} and if this is
     * an {@link Type#ERROR} or {@link Type#EXIT} action then it will also be logged. For {@link Type#ERROR} or
     * {@link Type#EXIT} actions this can be provided as part of the factory method.
     *
     * @param statusMessage to be reported
     * @return {@code this}
     */
    public Action setStatusMessage(String statusMessage) {
        m_statusMessage = statusMessage;
        return this;
    }

    /**
     * Add an arbitrary attachment to this action so that it can be retrieved upon the next
     * {@link Scheduler#getNextAction(ActionResult)} invocation.
     *
     * @param attachment object to attach. May be {@code null}
     * @return {@code this}
     * @throws IllegalArgumentException if the type of action is either {@link Type#EXIT} or {@link Type#ERROR}
     */
    public Action setAttachment(Object attachment) throws IllegalArgumentException {
        if (m_type == Type.ERROR || m_type == Type.EXIT) {
            throw new IllegalArgumentException("Cannot set attachment when action type is " + m_type);
        }
        m_scheduledAction.setAttachment(attachment);
        return this;
    }

    @Override
    public String toString() {
        return "Action [m_type=" + m_type + ", m_statusMessage=" + m_statusMessage + ", m_scheduledAction="
                + m_scheduledAction + "]";
    }

    ScheduledAction getScheduledAction() {
        return m_scheduledAction;
    }

    /**
     * Enum used to describe the type of the {@link Action}.
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
