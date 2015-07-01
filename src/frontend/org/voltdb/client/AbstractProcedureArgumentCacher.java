/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb.client;

/**
 * Implementation of a ProcedureArgumentCacher that can be extended by procedure callbacks
 *
 */
public abstract class AbstractProcedureArgumentCacher implements ProcedureArgumentCacher {

    private Object m_args[] = null;

    protected Object[] args() {
        return m_args;
    }

    /**
     * Invoked when a procedure is called to give the callback an opportunity to cache
     * the arguments
     * @param args Array of arguments passed to the stored procedure
     */
    public void setArgs(Object args[]) {
        m_args = args;
    }
}
