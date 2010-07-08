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
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.voltdb.VoltDB;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.utils.VoltLoggerFactory;

import java.util.HashSet;
import java.util.ArrayDeque;

/**
 * FaultDistributor routes VoltFaults from reporters to entities that have
 * registered their interest in particular types/subclasses of VoltFault.
 */
public class FaultDistributor implements FaultDistributorInterface, Runnable
{
    private static final Logger hostLog = Logger.getLogger("HOST", VoltLoggerFactory.instance());
    public FaultDistributor()
    {
        m_faultHandlers =
            new HashMap<FaultType, TreeMap<Integer, List<FaultHandlerData>>>();
        m_thread = new Thread(this, "Fault Distributor");
        m_thread.setDaemon(true);
        m_thread.start();
    }

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
    public synchronized void registerFaultHandler(FaultType type,
                                                  FaultHandler handler,
                                                  int order)
    {
        TreeMap<Integer, List<FaultHandlerData>> handler_map =
            m_faultHandlers.get(type);
        List<FaultHandlerData> handler_list = null;
        if (handler_map == null)
        {
            handler_map = new TreeMap<Integer, List<FaultHandlerData>>();
            m_faultHandlers.put(type, handler_map);
            handler_list = new ArrayList<FaultHandlerData>();
            handler_map.put(order, handler_list);
        }
        else
        {
            handler_list = handler_map.get(order);
            if (handler_list == null)
            {
                handler_list = new ArrayList<FaultHandlerData>();
                handler_map.put(order, handler_list);
            }
        }
        FaultHandlerData data = new FaultHandlerData(handler);
        handler_list.add(data);
        m_faultHandlersData.put(handler, data);
    }

    /**
     * A convenience method for registering a default handler.  Maybe this gets
     * blown up in the future --izzy
     * @param handler
     */
    public void registerDefaultHandler(FaultHandler handler)
    {
        // semi-arbitrarily large enough priority value so that
        // the default handler is last
        registerFaultHandler(FaultType.UNKNOWN, handler, 1000);
    }

    /**
     * Report that a fault (represented by the fault arg) has occurred.  All
     * registered FaultHandlers for that type will get called, sequenced from
     * lowest order to highest order, with no guaranteed order within an 'order'
     * (yes, horrible word overloading).  Any reported
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
        m_pendingFaults.offer(fault);
        this.notify();
    }

    @Override
    public synchronized void reportFaultHandled(FaultHandler handler, VoltFault fault)
    {
        m_handledFaults.offer(new HandledFault(handler, fault));
        this.notify();
    }

    /*
     * Check if this fault is a duplicate of a previously reported fault
     */
    private boolean duplicateCheck(VoltFault fault) {
        HashSet<VoltFault> knownFaults = m_knownFaults.get(fault.getFaultType());
        if (knownFaults == null) {
            knownFaults = new HashSet<VoltFault>();
            m_knownFaults.put(fault.getFaultType(), knownFaults);
        }
        if (knownFaults.add(fault)) {
            return false;
        }
        return true;
    }

    @Override
    public void shutDown() throws InterruptedException
    {
        m_thread.interrupt();
        m_thread.join();
    }

    private HashMap<FaultType, TreeMap<Integer, List<FaultHandlerData>>> m_faultHandlers;
    private HashMap<FaultHandler, FaultHandlerData> m_faultHandlersData = new HashMap<FaultHandler, FaultHandlerData> ();
    private HashMap<FaultType, HashSet<VoltFault>> m_knownFaults = new HashMap<FaultType, HashSet<VoltFault>>();
    private final ArrayDeque<VoltFault> m_pendingFaults = new ArrayDeque<VoltFault>();
    private final ArrayDeque<HandledFault> m_handledFaults = new ArrayDeque<HandledFault>();
    private Thread m_thread;

    class FaultHandlerData {

        FaultHandlerData(FaultHandler handler) { m_handler = handler; }
        FaultHandler m_handler;

        /*
         * Faults that have been passed to the handler but not handled
         */
        HashSet<VoltFault> m_pendingFaults = new HashSet<VoltFault>();
    }

    class HandledFault {
        HandledFault(FaultHandler handler, VoltFault fault) {
            m_handler = handler;
            m_fault = fault;
        }

        FaultHandler m_handler;
        VoltFault m_fault;
    }

    /*
     * Process notifications of faults that have been handled by their handlers. This removes
     * the fault from the set of outstanding faults for that handler.
     */
    private void processHandledFaults() {
        while (!m_handledFaults.isEmpty()) {
            HandledFault hf = m_handledFaults.poll();
            if (!m_faultHandlersData.containsKey(hf.m_handler)) {
                hostLog.fatal("A handled fault was reported for a handler that is not registered");
                VoltDB.crashVoltDB();
            }
            boolean removed = m_faultHandlersData.get(hf.m_handler).m_pendingFaults.remove(hf.m_fault);
            if (!removed) {
                hostLog.fatal("A handled fault was reported that was not pending for the provided handler");
                VoltDB.crashVoltDB();
            }
        }
    }

    /*
     * Dedupe incoming fault reports and then report the new fault along with outstanding faults
     * to any interested fault handlers.
     */
    private void processPendingFaults() {
        if (m_pendingFaults.isEmpty()) {
            return;
        }
        HashMap<FaultType, HashSet<VoltFault>> faultsMap  = new HashMap<FaultType, HashSet<VoltFault>>();
        while (!m_pendingFaults.isEmpty()) {
            VoltFault fault = m_pendingFaults.poll();
            if (duplicateCheck(fault)) {
                continue;
            }
            HashSet<VoltFault> faults = faultsMap.get(fault.getFaultType());
            if (faults == null) {
                faults = new HashSet<VoltFault>();
                faultsMap.put(fault.getFaultType(), faults);
            }
            boolean added = faults.add(fault);
            assert(added);
        }

        if (faultsMap.isEmpty()) {
            return;
        }

        for (Map.Entry<FaultType, HashSet<VoltFault>> entry : faultsMap.entrySet()) {
            TreeMap<Integer, List<FaultHandlerData>> handler_map =
                m_faultHandlers.get(entry.getKey());
            if (handler_map == null)
            {
                handler_map = m_faultHandlers.get(FaultType.UNKNOWN);
                if (handler_map == null)
                {
                    registerDefaultHandler(new DefaultFaultHandler());
                    handler_map = m_faultHandlers.get(FaultType.UNKNOWN);
                }
            }
            for (List<FaultHandlerData> handler_list : handler_map.values())
            {
                for (FaultHandlerData handlerData : handler_list)
                {
                    if (handlerData.m_pendingFaults.addAll(entry.getValue())) {
                        handlerData.m_handler.faultOccured(handlerData.m_pendingFaults);
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                synchronized (this) {
                    processHandledFaults();
                    processPendingFaults();
                    processHandledFaults();
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        } catch (Exception e) {
            hostLog.fatal("", e);
            VoltDB.crashVoltDB();
        }
    }
}
