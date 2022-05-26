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

import static java.util.Objects.requireNonNull;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Class which describes an interval, an action to perform after that interval and a callback which should be invoked
 * after the action has been performed.
 * <p>
 * This class also has an optional attachment which can be used to pass state between the initiating class and the
 * callback.
 */
public class ScheduledAction {
    private final long m_intervalNs;
    private final ActionDescription m_action;
    private final Function<ActionResult, ScheduledAction> m_callback;
    private Object m_attachment;

    /**
     * Create a {@link ScheduledAction} from a {@link Action} which is of type stop.
     *
     * @param action Base {@link Action} to get type and message from
     * @return A new {@link ScheduledAction} derived from {@code action}
     */
    public static ScheduledAction of(Action action) {
        if (!requireNonNull(action, "action").getType().isStop()) {
            throw new IllegalArgumentException("Action type must be stop");
        }

        return new ScheduledAction(-1, action, null);
    }

    /**
     * Create a {@link ScheduledAction} from a {@link Interval} and {@link Action}. The {@code action} must not be of
     * type stop.
     *
     * @param interval To wait until performing {@code action}
     * @param action   To be performed after {@code interval}
     * @return A new {@link ScheduledAction} from combining {@code interval} and {@code action}
     */
    public static ScheduledAction of(Interval interval, Action action) {
        requireNonNull(interval, "interval");
        if (requireNonNull(action, "action").getType().isStop()) {
            throw new IllegalArgumentException("Action type cannot be stop");
        }

        return new ScheduledAction(interval.getInterval(TimeUnit.NANOSECONDS), action, r -> {
            Action nextAction = action.getCallback().apply(r);
            if (nextAction.getType().isStop()) {
                return of(nextAction);
            }
            return of(interval.getCallback().apply(r), nextAction);
        });
    }

    /**
     * Create a {@link ScheduledAction} which indicates that an unrecoverable error has occurred and the scheduler must
     * exit.
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage To log indicating the details of the error. May be {@code null}
     * @return A new {@link ActionType#ERROR} instance of {@link ScheduledAction}
     */
    public static ScheduledAction error(String statusMessage) {
        return new ScheduledAction(-1, new ActionDescription(ActionType.ERROR, statusMessage, null), null);
    }

    /**
     * Create a {@link ScheduledAction} which indicates that the scheduler has reached the end of its life cycle
     * gracefully
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage To log indicating the details of the error. May be {@code null}
     * @return A new {@link ActionType#EXIT} instance of {@link ScheduledAction}
     */
    public static ScheduledAction exit(String statusMessage) {
        return new ScheduledAction(-1, new ActionDescription(ActionType.EXIT, statusMessage, null), null);
    }

    /**
     * Create a {@link ScheduledAction} which executes a procedure with given parameters after {@code interval}
     *
     * @param interval            Interval after which the procedure is to be executed
     * @param timeUnit            {#link TimeUnit} of {@code interval}
     * @param callback            That is invoked after {@code procedure} completes execution
     * @param procedure           Name of procedure to execute
     * @param procedureParameters To pass to procedure during execution
     * @return A new {@link ActionType#PROCEDURE} instance of {@link ScheduledAction}
     */
    public static ScheduledAction procedureCall(long interval, TimeUnit timeUnit,
            Function<ActionResult, ScheduledAction> callback, String procedure, Object... procedureParameters) {
        return new ScheduledAction(interval, requireNonNull(timeUnit, "timeUnit"),
                new ActionDescription(ActionType.PROCEDURE, null, requireNonNull(procedure, "procedure"),
                        requireNonNull(procedureParameters, "procedureParameters")),
                requireNonNull(callback, "callback"));
    }

    /**
     * Create a {@link ScheduledAction} which causes the {@code callback} to be invoked after {@code interval} . A
     * {@link ActionResult} will be associated with this call however it will have a {@code null} {@code procedure} and
     * {@code result} but an {@code attachment} can be used.
     *
     * @param interval Interval until the {@code callback} is invoked
     * @param timeUnit {@link TimeUnit} of {@code interval}
     * @param callback That is invoked after {@code interval} has passed
     * @return A new {@link ActionType#CALLBACK} instance of {@link ScheduledAction}
     */
    public static ScheduledAction callback(long interval, TimeUnit timeUnit,
            Function<ActionResult, ScheduledAction> callback) {
        return new ScheduledAction(interval, requireNonNull(timeUnit, "timeUnit"),
                new ActionDescription(ActionType.CALLBACK, null, null), requireNonNull(callback, "callback"));
    }

    private ScheduledAction(long interval, TimeUnit timeUnit, ActionDescription action,
            Function<ActionResult, ScheduledAction> callback) {
        this(timeUnit.toNanos(interval), action, callback);
    }

    private ScheduledAction(long intervalNs, ActionDescription action, Function<ActionResult, ScheduledAction> callback) {
        m_intervalNs = intervalNs;
        m_action = action;
        m_callback = callback;
    }

    /**
     * @param timeUnit Of the returned interval
     * @return The time interval until this action should be performed
     */
    public long getInterval(TimeUnit timeUnit) {
        return timeUnit == TimeUnit.NANOSECONDS ? m_intervalNs : timeUnit.convert(m_intervalNs, TimeUnit.NANOSECONDS);
    }

    /**
     * @return The {@link ActionType} of this action
     */
    public ActionType getType() {
        return m_action.getType();
    }

    /**
     * @return Optional status message provided with any action
     */
    public String getStatusMessage() {
        return m_action.getStatusMessage();
    }

    /**
     * Set the optional status message which will be reported in the statistics for a task and if this is an
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} action then it will also be logged. For
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} actions this can be provided as part of the factory method.
     *
     * @param statusMessage To be reported
     * @return {@code this}
     */
    public ScheduledAction setStatusMessage(String statusMessage) {
        m_action.setStatusMessage(statusMessage);
        return this;
    }

    /**
     * @return Name of procedure to execute. Will be {@code null} if this is not a {@link ActionType#PROCEDURE}
     */
    public String getProcedure() {
        return m_action.getProcedure();
    }

    /**
     * @return The parameters that are to be passed the the procedure returned by {@link #getProcedure()}
     */
    public Object[] getProcedureParameters() {
        return m_action.getProcedureParameters();
    }

    Object[] getRawProcedureParameters() {
        return m_action.getRawProcedureParameters();
    }

    /**
     * @return The callback to invoke after the {@code action} has been performed
     */
    public Function<ActionResult, ScheduledAction> getCallback() {
        return m_callback;
    }

    /**
     * Add an arbitrary attachment to this instance so that it can be retrieved from the {@link ActionResult}
     *
     * @param attachment Object to attach. May be {@code null}
     * @return {@code this}
     */
    public ScheduledAction setAttachment(Object attachment) {
        m_attachment = attachment;
        return this;
    }

    /**
     * Retrieve the optional attachment associated with this scheduled action
     *
     * @param <T> Type of attachment
     * @return The attachment or {@code null} of there was no attachment
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttachment() {
        return (T) m_attachment;
    }

    @Override
    public String toString() {
        return "ScheduledAction [intervalNs=" + m_intervalNs + ", action=" + m_action + ", callback=" + m_callback
                + ", attachment=" + m_attachment + "]";
    }
}
