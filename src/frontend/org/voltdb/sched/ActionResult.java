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

import java.util.concurrent.TimeUnit;

import org.voltdb.client.ClientResponse;

/**
 * The result of executing a {@link Action} returned by {@link Scheduler#getFirstAction()} or
 * {@link Scheduler#getNextAction(ActionResult)}
 */
public interface ActionResult {
    /**
     * @return The type of action which was performed
     */
    public Action.Type getType();

    /**
     * @param unit {@link TimeUnit} of delay returned by this method
     * @return Time delay in {@code timeUnit}
     */
    public long getDelay(TimeUnit unit);

    /**
     * @return Name of procedure to execute. May be {@code null} if this is a forced rerun of the {@link Scheduler}
     */
    public String getProcedure();

    /**
     * @return A The parameters that are to be passed the the procedure returned by {@link #getProcedure()}
     */
    public Object[] getProcedureParameters();

    /**
     * @return {@link ClientResponse} from the execution of {@link #getProcedure()}. Will be null if procedure has not
     *         be executed or this is a rerun of the scheduler
     */
    public ClientResponse getResponse();

    /**
     * Retrieve the attachment associated with this scheduled procedure
     *
     * @param <T> Type of attachment
     * @return The attachment or {@code null} of there was no attachment
     * @throws ClassCastException If the attachment is not of type {@code T}
     */
    public <T> T getAttachment() throws ClassCastException;
}
