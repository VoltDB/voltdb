/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.HashSet;

import org.voltdb.fault.VoltFault.FaultType;

public interface FaultDistributorInterface
{
    public enum PPDPolicyDecision {
        NodeFailure,
        PartitionDetection
    }


    /**
     * Register a FaultHandler object to be notified when FaultType type occurs.
     * @param order Where in the calling sequence of fault handlers this
     *        handler should appear.  Lower values will be called first; multiple
     *        handlers can have the same value but there is no guarantee of
     *        the order in which they will be called
     * @param handler The FaultHandler object which the caller wants called back
     *        when the the specified type occurs
     * @param types The FaultType in which the caller is interested
     */
    public abstract void registerFaultHandler(int order,
                                              FaultHandler handler,
                                              FaultType... types);

    /**
     * Report that a fault (represented by the fault arg) has occurred.  All
     * registered FaultHandlers for that type will get called. Any reported
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
     * Report that the fault has been handled by the specified handler and that
     * it should no longer be included in the list of outstanding faults. The report
     * is asynchronous so it is still possible for the fault to be delivered to the handler
     * if reportFaultHandled is invoked outside the handler.
     */
    public abstract void reportFaultHandled(FaultHandler handler, VoltFault fault);

    /**
     * Report that a fault (represented by the fault arg) has cleared.  All
     * registered FaultHandlers for that type will get called.
     *
     * @param fault The fault which is being reported as cleared
     */
    public abstract void reportFaultCleared(VoltFault fault);

    /**
     * Tell the fault distributor that the server is being shut down.
     * Prevents many false positives that prevent an orderly shutdown.
     */
    public abstract void shutDown() throws InterruptedException;

    /**
     * Allow fault manager to make partition detection decisons once
     * a fault set is agreed to by the execution site agreement process
     * @param newFailedSiteIds
     * @return
     */
    public abstract PPDPolicyDecision makePPDPolicyDecisions(HashSet<Integer> newFailedSiteIds);
}