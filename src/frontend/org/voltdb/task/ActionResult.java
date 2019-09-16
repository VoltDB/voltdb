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

import java.util.concurrent.TimeUnit;

import org.voltdb.client.ClientResponse;

/**
 * The result of executing an {@link Action} which was produed by either an {@link ActionSchedule} or
 * {@link ActionScheduler}
 */
public interface ActionResult {
    /**
     * @return The type of action which was performed
     */
    public ActionType getType();

    /**
     * Retrieve the delay which was applied to the executed action.
     *
     * @param timeUnit {@link TimeUnit} of delay returned by this method
     * @return Time delay in {@code timeUnit}
     */
    public long getDelay(TimeUnit timeUnit);

    /**
     * @return Name of procedure that was executed. Will be {@code null} if the action was a {@link ActionType#CALLBACK}
     */
    public String getProcedure();

    /**
     * @return A The parameters that were passed to the procedure returned by {@link #getProcedure()}
     */
    public Object[] getProcedureParameters();

    /**
     * @return {@link ClientResponse} from the execution of {@link #getProcedure()}. Will be {@code null} if the action
     * was a {@link ActionType#CALLBACK}
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
