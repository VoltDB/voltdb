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

import static java.util.Objects.requireNonNull;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Class which describes a delay, an action to perform after that delay and a callback which should be invoked after the
 * action has been performed.
 * <p>
 * This class also has an optional attachment which can be used to pass state between the initiating class and the
 * callback.
 */
public class DelayedAction {
    private final long m_delayNs;
    private final ActionDescription m_action;
    private final Function<ActionResult, DelayedAction> m_callback;
    private Object m_attachment;

    /**
     * Create a {@link DelayedAction} from a {@link Action} which is of type stop.
     *
     * @param action Base {@link Action} to get type and message from
     * @return A new {@link DelayedAction} derived from {@code action}
     */
    public static DelayedAction createStop(Action action) {
        if (!action.getType().isStop()) {
            throw new IllegalArgumentException("Action type must be stop");
        }

        return new DelayedAction(-1, action, null);
    }

    /**
     * Create a {@link DelayedAction} from a {@link ActionDelay} and {@link Action}. The {@code action} must not be of
     * type stop.
     *
     * @param delay  To wait until performing {@code action}
     * @param action To be performed after {@code delay}
     * @return A new {@link DelayedAction} from combining {@code delay} and {@code action}
     */
    public static DelayedAction create(ActionDelay delay, Action action) {
        if (action.getType().isStop()) {
            throw new IllegalArgumentException("Action type cannot be stop");
        }

        return new DelayedAction(delay.getDelay(TimeUnit.NANOSECONDS), action, r -> {
            Action nextAction = action.getCallback().apply(r);
            if (nextAction.getType().isStop()) {
                return createStop(nextAction);
            }
            return create(delay.getCallback().apply(r), nextAction);
        });
    }

    /**
     * Create a {@link DelayedAction} which indicates that an unrecoverable error has occurred and the scheduler must
     * exit.
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage To log indicating the details of the error. May be {@code null}
     * @return A new {@link ActionType#ERROR} instance of {@link DelayedAction}
     */
    public static DelayedAction createError(String statusMessage) {
        return new DelayedAction(-1, new ActionDescription(ActionType.ERROR, statusMessage, null), null);
    }

    /**
     * Create a {@link DelayedAction} which indicates that the scheduler has reached the end of its life cycle
     * gracefully
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage To log indicating the details of the error. May be {@code null}
     * @return A new {@link ActionType#EXIT} instance of {@link DelayedAction}
     */
    public static DelayedAction createExit(String statusMessage) {
        return new DelayedAction(-1, new ActionDescription(ActionType.EXIT, statusMessage, null), null);
    }

    /**
     * Create a {@link DelayedAction} which executes a procedure with given parameters after {@code delay}
     *
     * @param delay               Time for the procedure to be executed
     * @param timeUnit            {#link TimeUnit} of {@code delay}
     * @param callback            That is invoked after {@code procedure} completes execution
     * @param procedure           Name of procedure to execute
     * @param procedureParameters To pass to procedure during execution
     * @return A new {@link ActionType#PROCEDURE} instance of {@link DelayedAction}
     */
    public static DelayedAction createProcedure(long delay, TimeUnit timeUnit,
            Function<ActionResult, DelayedAction> callback, String procedure, Object... procedureParameters) {
        return new DelayedAction(delay, timeUnit,
                new ActionDescription(ActionType.PROCEDURE, null, requireNonNull(procedure),
                        requireNonNull(procedureParameters)),
                requireNonNull(callback));
    }

    /**
     * Create a {@link DelayedAction} which causes the {@code callback} to be invoked after {@code delay} . A
     * {@link ActionResult} will be associated with this call however it will have a {@code null} {@code procedure} and
     * {@code result} but an {@code attachment} can be used.
     *
     * @param delay    Delay until the {@code callback} is invoked
     * @param timeUnit {@link TimeUnit} of {@code delay}
     * @param callback That is invoked after {@code delay} has passed
     * @return A new {@link ActionType#CALLBACK} instance of {@link DelayedAction}
     */
    public static DelayedAction createCallback(long delay, TimeUnit timeUnit,
            Function<ActionResult, DelayedAction> callback) {
        return new DelayedAction(delay, timeUnit, new ActionDescription(ActionType.CALLBACK, null, null),
        requireNonNull(callback));
    }

    private DelayedAction(long delay, TimeUnit timeUnit, ActionDescription action,
            Function<ActionResult, DelayedAction> callback) {
        this(timeUnit.toNanos(delay), action, callback);
    }

    private DelayedAction(long delayNs, ActionDescription action, Function<ActionResult, DelayedAction> callback) {
        m_delayNs = delayNs;
        m_action = action;
        m_callback = callback;
    }

    /**
     * @param timeUnit Of the returned delay
     * @return The delay until this action should be performed
     */
    public long getDelay(TimeUnit timeUnit) {
        return timeUnit == TimeUnit.NANOSECONDS ? m_delayNs : timeUnit.convert(m_delayNs, TimeUnit.NANOSECONDS);
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
     * Set the optional status massage which will be reported in the statistics for a task and if this is an
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} action then it will also be logged. For
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} actions this can be provided as part of the factory method.
     *
     * @param statusMessage To be reported
     * @return {@code this}
     */
    public DelayedAction setStatusMessage(String statusMessage) {
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
    public Function<ActionResult, DelayedAction> getCallback() {
        return m_callback;
    }

    /**
     * Add an arbitrary attachment to this instance so that it can be retrieved from the {@link ActionResult}
     *
     * @param attachment Object to attach. May be {@code null}
     * @return {@code this}
     */
    public DelayedAction setAttachment(Object attachment) {
        m_attachment = attachment;
        return this;
    }

    /**
     * Retrieve the optional attachment associated with this delayed action
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
        return "DelayedAction [delayNs=" + m_delayNs + ", action=" + m_action + ", callback=" + m_callback
                + ", attachment=" + m_attachment + "]";
    }
}
