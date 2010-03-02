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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.voltdb.fault.VoltFault.FaultType;

/**
 * FaultDistributor routes VoltFaults from reporters to entities that have
 * registered their interest in particular types/subclasses of VoltFault.
 */
public class FaultDistributor
{
    public FaultDistributor()
    {
        m_faultHandlers = new HashMap<FaultType, List<FaultHandler>>();
    }

    /**
     * Register a FaultHandler object to be notified when FaultType type occurs.
     *
     * @param type The FaultType in which the caller is interested
     * @param handler The FaultHandler object which the caller wants called back
     *        when the the specified type occurs
     */
    public synchronized void registerFaultHandler(FaultType type, FaultHandler handler)
    {
        List<FaultHandler> handler_list = m_faultHandlers.get(type);
        if (handler_list == null)
        {
            handler_list = new ArrayList<FaultHandler>();
            m_faultHandlers.put(type, handler_list);
        }
        handler_list.add(handler);
    }

    /**
     * A convenience method for registering a default handler.  Maybe this gets
     * blown up in the future --izzy
     * @param handler
     */
    public void registerDefaultHandler(FaultHandler handler)
    {
        registerFaultHandler(FaultType.UNKNOWN, handler);
    }

    /**
     * Report that a fault (represted by the fault arg) has occured.  All
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
    // XXX-FAILURE need more error checking, default handling, and whatnot
    public synchronized void reportFault(VoltFault fault)
    {
        List<FaultHandler> handler_list =
            m_faultHandlers.get(fault.getFaultType());
        if (handler_list == null)
        {
            handler_list = m_faultHandlers.get(FaultType.UNKNOWN);
            if (handler_list == null)
            {
                registerDefaultHandler(new DefaultFaultHandler());
                handler_list = m_faultHandlers.get(FaultType.UNKNOWN);
            }
        }
        for (FaultHandler handler : handler_list)
        {
            handler.faultOccured(fault);
        }
    }

    private HashMap<FaultType, List<FaultHandler>> m_faultHandlers;
}
