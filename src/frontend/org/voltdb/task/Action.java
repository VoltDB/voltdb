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

import java.util.function.Function;

/**
 * Class which defines an action to be taken as well as a callback to be invoked when that action has been performed.
 * There is no callback present if the type of action is a stop action.
 */
public final class Action extends ActionDescription {
    private final Function<ActionResult, Action> m_callback;

    /**
     * Create an {@link Action} which indicates that an unrecoverable error has occurred and the scheduler must exit.
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage To log indicating the details of the error. May be {@code null}
     * @return A new {@link ActionType#ERROR} instance of {@link Action}
     */
    public static Action error(String statusMessage) {
        return new Action(ActionType.ERROR, statusMessage, null, null);
    }

    /**
     * Create an {@link Action} which indicates that the scheduler has reached the end of its life cycle gracefully
     * <p>
     * {@code statusMessage} is the same status message which is set by calling {@link #setStatusMessage(String)}
     *
     * @param statusMessage To log indicating the details of the error. May be {@code null}
     * @return A new {@link ActionType#EXIT} instance of {@link Action}
     */
    public static Action exit(String statusMessage) {
        return new Action(ActionType.EXIT, statusMessage, null, null);
    }

    /**
     * Create an {@link Action} which executes a procedure with given parameters
     *
     * @param callback            To be invoked after this procedure is executed
     * @param procedure           Name of procedure to execute
     * @param procedureParameters That are passed to procedure for execution
     * @return A new {@link ActionType#PROCEDURE} instance of {@link Action}
     */
    public static Action procedureCall(Function<ActionResult, Action> callback, String procedure,
            Object... procedureParameters) {
        return new Action(ActionType.PROCEDURE, null, requireNonNull(callback, "callback"),
                requireNonNull(procedure, "procedure"), requireNonNull(procedureParameters, "procedureParameters"));
    }

    /**
     * Create an {@link Action} which causes the {@code callback} to be invoked. An {@link ActionResult} will be
     * associated with this call however it will have a {@code null} {@code procedure} and {@code result}.
     *
     * @param callback To be invoked for this action
     * @return A new {@link ActionType#CALLBACK} instance of {@link Action}
     */
    public static Action callback(Function<ActionResult, Action> callback) {
        return new Action(ActionType.CALLBACK, null, requireNonNull(callback, "callback"), null);
    }

    private Action(ActionType type, String statusMessage, Function<ActionResult, Action> callback, String procedure,
            Object... procedureParameters) {
        super(type, statusMessage, procedure, procedureParameters);
        m_callback = callback;
    }

    /**
     * Set the optional status massage which will be reported in the statistics for a task and if this is an
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} action then it will also be logged. For
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} actions this can be provided as part of the factory method.
     *
     * @param statusMessage To be reported
     * @return {@code this}
     */
    @Override
    public Action setStatusMessage(String statusMessage) {
        super.setStatusMessage(statusMessage);
        return this;
    }

    /**
     * @return callback to be invoked after any provided procedure is executed. Will be {@code null} if the type of
     *         action is a stop action
     */
    public Function<ActionResult, Action> getCallback() {
        return m_callback;
    }

    @Override
    public String toString() {
        return "Action [m_callback=" + m_callback + ", " + super.toString() + "]";
    }
}
