/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltcore.logging.VoltLogger;

/**
 * FaultDistributor routes VoltFaults from reporters to entities that have
 * registered their interest in particular types/subclasses of VoltFault.
 */
public class FaultDistributor implements FaultDistributorInterface, Runnable
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // A list of registered handlers for each fault type.
    private final HashMap<FaultType, TreeMap<Integer, List<FaultHandler>>> m_faultHandlers;

    // A list of faults that at least one handler has not reported handled.
    private ArrayDeque<VoltFault> m_pendingFaults = new ArrayDeque<VoltFault>();

    // Fault distributer runs in this thread
    private final Thread m_thread;

    // If true, concurrent failures of 1/2 or more of the current node set, leaving behind
    // a durable survivor set will cause the survivors to checkpoint and die.
    private final boolean m_partitionDetectionEnabled;

    // True if a partition detection occurred. All future faults are ignored
    private volatile boolean m_partitionDetectionTriggered;

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
            new HashMap<FaultType, TreeMap<Integer, List<FaultHandler>>>();
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
    @Override
    public synchronized void registerFaultHandler(int order,
                                                  FaultHandler handler,
                                                  FaultType... types)
    {
        FaultType[] typeArray = types;
        for (FaultType type : typeArray) {
            TreeMap<Integer, List<FaultHandler>> handler_map = m_faultHandlers.get(type);
            List<FaultHandler> handler_list = null;
            // first handler for this fault type?
            if (handler_map == null)
            {
                handler_map = new TreeMap<Integer, List<FaultHandler>>();
                m_faultHandlers.put(type, handler_map);
                handler_list = new ArrayList<FaultHandler>();
                handler_map.put(order, handler_list);
            }
            else
            {
                handler_list = handler_map.get(order);
                if (handler_list == null)
                {
                    handler_list = new ArrayList<FaultHandler>();
                    handler_map.put(order, handler_list);
                }
            }

            // handler map now has entry for this fault type
            // and handler_list is the list of handlers for this fault type
            // so add the new handler to the list, associated to the
            // corresponding fault handler data.
            handler_list.add(handler);

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

    @Override
    public void shutDown() throws InterruptedException
    {
        m_thread.interrupt();
        m_thread.join();
    }

    /*
     * Take newly reported faults and pass them to fault handler
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

        // Group the new faults by FaultType
        HashMap<FaultType, HashSet<VoltFault>> postFilterFaults =
            organizeNewFaults(pendingFaults);

        // No action if all faults were known/filtered
        // xxx-izzy I don't think the .isEmpty() predicate is possible now
        if (postFilterFaults == null || postFilterFaults.isEmpty()) {
            return;
        }

        for (Map.Entry<FaultType, HashSet<VoltFault>> entry : postFilterFaults.entrySet()) {
            TreeMap<Integer, List<FaultHandler>> handler_map =
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
            for (List<FaultHandler> handler_list : handler_map.values())
            {
                for (FaultHandler handler : handler_list)
                {
                    handler.faultOccured(entry.getValue());
                }
            }
        }
    }


    /*
     * Return a map of FaultType -> fault set containing the new faults
     * Short-circuit the process if we've already triggered partition detection
     * due to a prior set of faults
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

    @Override
    public void run() {
        try {
            while (true) {
                processPendingFaults();
                synchronized (this) {
                    if (m_pendingFaults.isEmpty()) {
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
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }

    @Override
    public PPDPolicyDecision makePPDPolicyDecisions(HashSet<Long> newFailedSiteIds, SiteTracker tracker)
    {
        if (!m_partitionDetectionEnabled) {
            return PPDPolicyDecision.NodeFailure;
        }

        // if there are no node faults, there is no work to do.
        if (newFailedSiteIds == null || newFailedSiteIds.size() == 0) {
            return PPDPolicyDecision.NodeFailure;
        }

        // collapse failed sites into failed hosts
        final HashSet<Integer> failedHosts = new HashSet<Integer>();
        for (Long siteId : newFailedSiteIds) {
            failedHosts.add(SiteTracker.getHostForSite(siteId));
        }

        // because tracker already represents newFailedSiteId failures...
        final int prevSurvivorCnt = tracker.getAllHosts().size() + failedHosts.size();

        // find the lowest hostId between the still-alive hosts and the
        // failed hosts. Which set contains the lowest hostId?
        int blessedHostId = Integer.MAX_VALUE;
        boolean blessedHostIdInFailedSet = true;

        for (Integer hostId : failedHosts) {
            if (hostId < blessedHostId) {
                blessedHostId = hostId;
            }
        }

        for (Integer hostId : tracker.getAllHosts()) {
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
