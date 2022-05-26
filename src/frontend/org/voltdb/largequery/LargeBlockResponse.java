/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.largequery;

/**
 * This class represents the response produced when executing a LargeBlockTask.
 */
public class LargeBlockResponse {
    private final Exception m_exception;

    /**
     * Construct a LargeBlockResponse for a task that failed
     * with the given exception
     * @param exc   the exception that caused the task to fail
     */
    LargeBlockResponse(Exception exc) {
        m_exception = exc;
    }

    /**
     * Construct a LargeBlockResponse for a task that was successful
     */
    LargeBlockResponse() {
        m_exception = null;
    }

    /**
     * Tells callers if the task was successful
     * @return   true iff the task was successful
     */
    public boolean wasSuccessful() {
        return m_exception == null;
    }

    /**
     * Provides callers with the exception that caused the task to fail, if any
     * @return   the exception that caused the task to fail (may be null)
     */
    public Exception getException() {
        return m_exception;
    }
}
