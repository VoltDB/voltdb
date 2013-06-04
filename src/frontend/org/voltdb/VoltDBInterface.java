/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltdb;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.Pair;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.licensetool.LicenseApi;

import com.google.common.util.concurrent.ListeningExecutorService;

public interface VoltDBInterface
{
    public boolean rejoining();
    public boolean rejoinDataPending();

    /**
     * Invoked from the command log once this node is marked unfaulted.
     * Allows its command log to be used for recovery.
     * @param requestId The id, if any, associated with the truncation request.
     */
    public void recoveryComplete(String requestId);

    public void readBuildInfo(String editionTag);

    public CommandLog getCommandLog();

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    public void initialize(VoltDB.Configuration config);

    /**
     * Start all the site's event loops. That's it.
     */
    public void run();

    /**
     * Try to shut everything down so they system is ready to call
     * initialize again.
     * @param mainSiteThread The thread that m_inititalized the VoltDB or
     * null if called from that thread.
     * @return
     */
    public boolean shutdown(Thread mainSiteThread) throws InterruptedException;

    public void startSampler();

    public VoltDB.Configuration getConfig();
    public CatalogContext getCatalogContext();
    public String getBuildString();
    public String getVersionString();
    public HostMessenger getHostMessenger();
    public ArrayList<ClientInterface> getClientInterfaces();
    public Map<Long, ExecutionSite> getLocalSites();
    public OpsAgent getOpsAgent(OpsSelector selector);
    // Keep this method to centralize the cast to StatsAgent for
    // existing code
    public StatsAgent getStatsAgent();
    public MemoryStats getMemoryStatsSource();
    public FaultDistributorInterface getFaultDistributor();
    public BackendTarget getBackendTargetType();
    public String getLocalMetadata();
    public SiteTracker getSiteTrackerForSnapshot();

    /**
     * Update the global logging context in the server.
     *
     * @param xmlConfig The xml string containing the new logging configuration
     * @param currentTxnId  The transaction ID at which this method is called
     */
    void logUpdate(String xmlConfig, long currentTxnId);

    /**
     * Updates the catalog context stored by this VoltDB without destroying the old one,
     * in case anything still links to it.
     *
     * @param newCatalogBytes The catalog bytes.
     * @param diffCommands The commands to update the current catalog to the new one.
     * @param expectedCatalogVersion The version of the catalog the commands are targeted for.
     * @param currentTxnId  The transaction ID at which this method is called
     * @param deploymentCRC The CRC of the deployment file
     */
    public Pair<CatalogContext, CatalogSpecificPlanner> catalogUpdate(String diffCommands,
            byte[] newCatalogBytes, byte[] catalogBytesHash, int expectedCatalogVersion,
            long currentTxnId, long currentTxnTimestamp, long deploymentCRC);

   /**
     * Tells if the VoltDB is running. m_isRunning needs to be set to true
     * when the run() method is called, and set to false when shutting down.
     *
     * @return true if the VoltDB is running.
     */
    public boolean isRunning();

    /**
     * Notify RealVoltDB that recovery is complete
     */
    void onExecutionSiteRejoinCompletion(long transferred);

    /**
     * Set the operational mode this server should be in once it has finished
     * initialization. It's not set immediately because command log replay might
     * be in progress. But it will be set to the specified mode once replay has
     * finished. This method shouldn't be used once the server has finished
     * initialization.
     *
     * @param mode
     */
    public void setStartMode(OperationMode mode);

    public OperationMode getStartMode();

    public void setReplicationRole(ReplicationRole role);

    public ReplicationRole getReplicationRole();

    public void setReplicationActive(boolean active);

    public boolean getReplicationActive();

    /**
     * Set the operation mode of this server.
     * @param mode the operational mode to enter
     */
    public void setMode(OperationMode mode);

    /**
     * @return The operational mode this server is in
     */
    public OperationMode getMode();

    public SnapshotCompletionMonitor getSnapshotCompletionMonitor();

    /**
     * Schedule a work to be performed once or periodically.
     * No blocking or resource intensive work should be done
     * from this thread. High priority tasks,
     * that are known not to do anything risky can use
     * schedulePriorityWork if they actually have fine grained requirements.
     * All others should use schedule work
     * and be aware that they may stomp on each other.
     *
     * @param work
     *            The work to be scheduled
     * @param initialDelay
     *            The initial delay before the first execution of the work
     * @param delay
     *            The delay between each subsequent execution of the work. If
     *            this is negative, the work will only be executed once after
     *            the initial delay.
     * @param unit
     *            Time unit
     */
    public ScheduledFuture<?> scheduleWork(Runnable work, long initialDelay, long delay,
                             TimeUnit unit);

    /**
     * Schedule a work to be performed once or periodically.
     * This is for high priority work with fine grained scheduling requirements.
     * Tasks submitted here must absolutely not do any work in the scheduler thread.
     * Submit the work to be done to a different thread unless it is absolutely trivial.
     *
     * @param work
     *            The work to be scheduled
     * @param initialDelay
     *            The initial delay before the first execution of the work
     * @param delay
     *            The delay between each subsequent execution of the work. If
     *            this is negative, the work will only be executed once after
     *            the initial delay.
     * @param unit
     *            Time unit
     */
    public ScheduledFuture<?> schedulePriorityWork(Runnable work, long initialDelay, long delay,
                             TimeUnit unit);

    /**
     * Return an executor service for running non-blocking but computationally expensive
     * tasks.
     */
    public ListeningExecutorService getComputationService();

    /**
     * Return the license api. This may be null in community editions!
     */
     public LicenseApi getLicenseApi();

     public boolean isIV2Enabled();
}
