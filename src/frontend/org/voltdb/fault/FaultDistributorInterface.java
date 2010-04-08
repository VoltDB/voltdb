/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.fault;

import org.voltdb.fault.VoltFault.FaultType;

public interface FaultDistributorInterface
{

    /**
     * Register a FaultHandler object to be notified when FaultType type occurs.
     *
     * @param type The FaultType in which the caller is interested
     * @param handler The FaultHandler object which the caller wants called back
     *        when the the specified type occurs
     * @param order Where in the calling sequence of fault handlers this
     *        handler should appear.  Lower values will be called first; multiple
     *        handlers can have the same value but there is no guarantee of
     *        the order in which they will be called
     */
    public abstract void registerFaultHandler(FaultType type,
                                              FaultHandler handler,
                                              int order);

    /**
     * Report that a fault (represented by the fault arg) has occurred.  All
     * registered FaultHandlers for that type will get called, currently in
     * no guaranteed order (this will likely change in the future).  Any reported
     * fault for which there is no registered handler will be handled by
     * any registered handlers for the UNKNOWN fault type.  If there is no
     * registered handler for the UNKNOWN fault type, a DefaultFaultHandler will
     * be installed and called, which has the end result of
     * calling VoltDB.crashVoltDB().
     *
     * @param fault The fault which is being reported
     */
    public abstract void reportFault(VoltFault fault);

    /**
     * Tell the fault distributor that the server is being shut down.
     * Prevents many false positives that prevent an orderly shutdown.
     */
    public abstract void shutDown();
}