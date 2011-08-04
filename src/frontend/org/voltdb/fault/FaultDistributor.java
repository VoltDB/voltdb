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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.logging.VoltLogger;

/**
 * FaultDistributor routes VoltFaults from reporters to entities that have
 * registered their interest in particular types/subclasses of VoltFault.
 */
public class FaultDistributor implements FaultDistributorInterface, Runnable
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // A list of registered handlers for each fault type.
    private HashMap<FaultType, TreeMap<Integer, List<FaultHandlerData>>> m_faultHandlers;

    // A set of unhandled faults by fault handler
    private HashMap<FaultHandler, FaultHandlerData> m_faultHandlersData = new HashMap<FaultHandler, FaultHandlerData> ();

    // A list of already-seen faults by fault type
    private HashMap<FaultType, HashSet<VoltFault>> m_knownFaults = new HashMap<FaultType, HashSet<VoltFault>>();

    // A list of faults that at least one handler has not reported handled.
    private ArrayDeque<VoltFault> m_pendingFaults = new ArrayDeque<VoltFault>();

    // A list of handled (handler, fault) pairs
    private ArrayDeque<HandledFault> m_handledFaults = new ArrayDeque<HandledFault>();

    // Faults waiting to be cleared organized by FaultType
    private HashMap<FaultType, HashSet<VoltFault>> m_pendingClearedFaults  = new HashMap<FaultType, HashSet<VoltFault>>();

    // Fault distributer runs in this thread
    private Thread m_thread;

    // If true, concurrent failures of 1/2 or more of the current node set, leaving behind
    // a durable survivor set will cause the survivors to checkpoint and die.
    private final boolean m_partitionDetectionEnabled;

    // True if a partition detection occurred. All future faults are ignored
    private volatile boolean m_partitionDetectionTriggered;

    // Pairs a fault handlers to its specific unhandled fault set
    class FaultHandlerData {

        FaultHandlerData(FaultHandler handler) { m_handler = handler; }
        FaultHandler m_handler;

        // Faults that have been passed to the handler but not handled
        HashSet<VoltFault> m_handlersPendingFaults = new HashSet<VoltFault>();
    }

    // Pairs a fault handler to an instance of a fault
    class HandledFault {
        HandledFault(FaultHandler handler, VoltFault fault) {
            m_handler = handler;
            m_fault = fault;
        }

        FaultHandler m_handler;
        VoltFault m_fault;
    }

    /**
     * Create a FaultDistributor with default fault processing policies
     * @param voltdb
     */
    public FaultDistributor(VoltDBInterface voltdb) {
        this(voltdb,
             voltdb.getCatalogContext().cluster.getNetworkpartition());
    }

    /**
     * Create a FaultDistributor with specified fault processing policies
     * @param voltdb
     * @param enablePartitionDetectionPolicy convert appropriate node failure faults into
     * partition detection faults
     */
    public FaultDistributor(VoltDBInterface voltdb, boolean enablePartitionDetectionPolicy)
    {
        m_partitionDetectionEnabled = enablePartitionDetectionPolicy;
        m_faultHandlers =
            new HashMap<FaultType, TreeMap<Integer, List<FaultHandlerData>>>();
        for (VoltFault.FaultType type : VoltFault.FaultType.values()) {
            m_knownFaults.put( type, new HashSet<VoltFault>());
        }
        m_thread = new Thread(this, "Fault Distributor");
        m_thread.setDaemon(true);
        m_thread.start();
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
    public synchronized void registerFaultHandler(int order,
                                                  FaultHandler handler,
                                                  FaultType... types)
    {
        FaultType[] typeArray = types;
        for (FaultType type : typeArray) {
            TreeMap<Integer, List<FaultHandlerData>> handler_map = m_faultHandlers.get(type);
            List<FaultHandlerData> handler_list = null;
            // first handler for this fault type?
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

            // first fault type for this handler?
            FaultHandlerData data = m_faultHandlersData.get(handler);
            if (data == null) {
                data = new FaultHandlerData(handler);
                m_faultHandlersData.put(handler, data);
            }

            // handler map now has entry for this fault type
            // and handler_list is the list of handlers for this fault type
            // so add the new handler to the list, associated to the
            // corresponding fault handler data.
            handler_list.add(data);

        }
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
        registerFaultHandler(1000, handler, FaultType.UNKNOWN);
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
    @Override
    // XXX-FAILURE need more error checking, default handling, and whatnot
    public synchronized void reportFault(VoltFault fault)
    {
        m_pendingFaults.offer(fault);
        this.notify();
    }

    /**
     * Report that the fault condition has been cleared
     */
    @Override
    public synchronized void reportFaultCleared(VoltFault fault) {
        HashSet<VoltFault> faults = m_pendingClearedFaults.get(fault.getFaultType());
        if (faults == null) {
            faults = new HashSet<VoltFault>();
            m_pendingClearedFaults.put(fault.getFaultType(), faults);
        }
        boolean added = faults.add(fault);
        assert(added);
        this.notify();
    }

    @Override
    public synchronized void reportFaultHandled(FaultHandler handler, VoltFault fault)
    {
        m_handledFaults.offer(new HandledFault(handler, fault));
        this.notify();
    }

    @Override
    public void shutDown() throws InterruptedException
    {
        m_thread.interrupt();
        m_thread.join();
    }


    /*
     * Process notifications of faults that have been handled by their handlers. This removes
     * the fault from the set of outstanding faults for that handler.
     */
    void processHandledFaults() {
        ArrayDeque<HandledFault> handledFaults;
        synchronized (this) {
            if (m_handledFaults.isEmpty()) {
                return;
            }
            handledFaults = m_handledFaults;
            m_handledFaults = new ArrayDeque<HandledFault>();
        }
        while (!handledFaults.isEmpty()) {
            HandledFault hf = handledFaults.poll();
            if (!m_faultHandlersData.containsKey(hf.m_handler)) {
                hostLog.fatal("A handled fault was reported for a handler that is not registered");
                VoltDB.crashVoltDB();
            }
            boolean removed = m_faultHandlersData.get(hf.m_handler).m_handlersPendingFaults.remove(hf.m_fault);
            if (!removed) {
                hostLog.fatal("A handled fault was reported that was not pending for the provided handler");
                VoltDB.crashVoltDB();
            }
        }
    }

    /*
     * Check if this fault is a duplicate of a previously reported fault
     */
    private boolean isKnownFault(VoltFault fault) {
        assert (Thread.currentThread() == m_thread);
        return !m_knownFaults.get(fault.getFaultType()).add(fault);
    }

    /*
     * Dedupe incoming fault reports and then report the new fault along with outstanding faults
     * to any interested fault handlers.
     */
    void processPendingFaults() {
        ArrayDeque<VoltFault> pendingFaults;
        synchronized (this) {
            if (m_pendingFaults.isEmpty()) {
                return;
            }
            pendingFaults = m_pendingFaults;
            m_pendingFaults = new ArrayDeque<VoltFault>();
        }

        // Examine the known faults and the new faults
        HashMap<FaultType, HashSet<VoltFault>> postFilterFaults =
            organizeNewFaults(pendingFaults);

        // No action if all faults were known/filtered
        if (postFilterFaults == null || postFilterFaults.isEmpty()) {
            return;
        }

        for (Map.Entry<FaultType, HashSet<VoltFault>> entry : postFilterFaults.entrySet()) {
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
                    if (handlerData.m_handlersPendingFaults.addAll(entry.getValue())) {
                        handlerData.m_handler.faultOccured(handlerData.m_handlersPendingFaults);
                    }
                }
            }
        }
    }


    /*
     * Pre-process the pending faults list. Update known faults and return a
     * map of FaultType -> fault set containing the new faults (post-filtering).
     */
    private HashMap<FaultType, HashSet<VoltFault>>
    organizeNewFaults(ArrayDeque<VoltFault> pendingFaults)
    {
        if (m_partitionDetectionTriggered) {
            return null;
        }

        HashMap<FaultType, HashSet<VoltFault>> faultsMap = new HashMap<FaultType, HashSet<VoltFault>>();
        while (!pendingFaults.isEmpty()) {
            VoltFault fault = pendingFaults.poll();
            if (isKnownFault(fault)) {
                hostLog.debug("Fault is being dropped because it is already known " + fault);
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
        return faultsMap;
    }

    private void processClearedFaults() {
        HashMap<FaultType, HashSet<VoltFault>> pendingClearedFaults;
        synchronized (this) {
            if (m_pendingClearedFaults.isEmpty()) {
                return;
            }
            pendingClearedFaults = m_pendingClearedFaults;
            m_pendingClearedFaults = new HashMap<FaultType, HashSet<VoltFault>>();
        }

        for (Map.Entry<FaultType, HashSet<VoltFault>> entry : pendingClearedFaults.entrySet()) {
            // Remove the (FaultType, VoltFault) pairs from m_knownFaults
            HashSet<VoltFault> faults = entry.getValue();
            for (VoltFault fault : faults) {
                m_knownFaults.get(fault.getFaultType()).remove(fault);
            }

            // Clear the fault from each registered handler
            TreeMap<Integer, List<FaultHandlerData>> handler_map = m_faultHandlers.get(entry.getKey());
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
                    handlerData.m_handler.faultCleared(entry.getValue());
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                processHandledFaults();
                processPendingFaults();
                processClearedFaults();
                synchronized (this) {
                    if (m_pendingFaults.isEmpty() && m_handledFaults.isEmpty() && m_pendingClearedFaults.isEmpty()) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            hostLog.fatal("", e);
            VoltDB.crashVoltDB();
        }
    }

    @Override
    public PPDPolicyDecision makePPDPolicyDecisions(HashSet<Integer> newFailedSiteIds)
    {
        if (!m_partitionDetectionEnabled) {
            return PPDPolicyDecision.NodeFailure;
        }

        // if there are no node faults, there is no work to do.
        if (newFailedSiteIds == null || newFailedSiteIds.size() == 0) {
            return PPDPolicyDecision.NodeFailure;
        }

        // this tracker *is already updated* with the new failed site IDs
        final SiteTracker tracker = VoltDB.instance().getCatalogContext().siteTracker;

        // collapse failed sites into failed hosts
        final HashSet<Integer> failedHosts = new HashSet<Integer>();
        for (Integer siteId : newFailedSiteIds) {
            failedHosts.add(tracker.getHostForSite(siteId));
        }

        // because tracker already represents newFailedSiteId failures...
        final int prevSurvivorCnt = tracker.getAllLiveHosts().size() + failedHosts.size();

        // find the lowest hostId between the still-alive hosts and the
        // failed hosts. Which set contains the lowest hostId?
        int blessedHostId = Integer.MAX_VALUE;
        boolean blessedHostIdInFailedSet = true;

        for (Integer hostId : failedHosts) {
            if (hostId < blessedHostId) {
                blessedHostId = hostId;
            }
        }

        for (Integer hostId : tracker.getAllLiveHosts()) {
            if (hostId < blessedHostId) {
                blessedHostId = hostId;
                blessedHostIdInFailedSet = false;
            }
        }

        // Evaluate PPD triggers.

        // Exact 50-50 splits. The set with the lowest survivor host doesn't trigger PPD
        // If the blessed host is in the failure set, this set is not blessed.
        if (failedHosts.size() * 2 == prevSurvivorCnt) {
            if (blessedHostIdInFailedSet) {
                hostLog.info("Partition detection triggered for 50/50 cluster failure. " +
                             "This survivor set is shutting down.");
                m_partitionDetectionTriggered = true;
                return PPDPolicyDecision.PartitionDetection;
            }
            else {
                hostLog.info("Partition detected for 50/50 failure. " +
                             "This survivor set is continuing execution.");
                return PPDPolicyDecision.NodeFailure;
            }
        }

        // A strict, viable minority is always a partition.
        if (failedHosts.size() * 2 > prevSurvivorCnt) {
            hostLog.info("Partition detection triggered. " +
                         "This minority survivor set is shutting down.");
            m_partitionDetectionTriggered = true;
            return PPDPolicyDecision.PartitionDetection;
        }

        // all remaining cases are normal node failures
        return PPDPolicyDecision.NodeFailure;
    }

    /*
     * Check if the directory specified for the snapshot on partition detection
     * exists, and has permissions set correctly.
     */
    public boolean testPartitionDetectionDirectory(SnapshotSchedule schedule) {
        if (m_partitionDetectionEnabled) {
            File partitionPath = new File(schedule.getPath());
            if (!partitionPath.exists()) {
                hostLog.error("Directory " + partitionPath + " for partition detection snapshots does not exist");
                return false;
            }
            if (!partitionPath.isDirectory()) {
                hostLog.error("Directory " + partitionPath + " for partition detection snapshots is not a directory");
                return false;
            }
            File partitionPathFile = new File(partitionPath, Long.toString(System.currentTimeMillis()));
            try {
                partitionPathFile.createNewFile();
                partitionPathFile.delete();
            } catch (IOException e) {
                hostLog.error(
                        "Could not create a test file in " +
                        partitionPath +
                        " for partition detection snapshots");
                e.printStackTrace();
                return false;
            }
            return true;
        } else {
            return true;
        }
    }
}
