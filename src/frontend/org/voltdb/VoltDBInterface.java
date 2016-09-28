/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.Pair;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.settings.ClusterSettings;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

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
    public boolean isRunningWithOldVerbs();

    public String getVoltDBRootPath(PathsType.Voltdbroot path);
    public String getCommandLogPath(PathsType.Commandlog path);
    public String getCommandLogSnapshotPath(PathsType.Commandlogsnapshot path);
    public String getSnapshotPath(PathsType.Snapshots path);
    public String getExportOverflowPath(PathsType.Exportoverflow path);
    public String getDROverflowPath(PathsType.Droverflow path);

    public String getVoltDBRootPath();
    public String getCommandLogSnapshotPath();
    public String getCommandLogPath();
    public String getSnapshotPath();
    public String getExportOverflowPath();
    public String getDROverflowPath();

    public boolean isBare();
    /**
     * Initialize all the global components, then initialize all the m_sites.
     * @param config Configuration from command line.
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

    /**
     * Check if the host is in prepare-shutting down state.
     */
    public boolean isShuttingdown();

    /**
     * Set the host to be in shutting down state.When a host is in teh state of being shut down.
     * All reads and writes except the system stored procedures which are allowed as
     *specified in SystemProcedureCatalog will be blocked.
     */
    public void setShuttingdown(boolean shuttingdown);
    boolean isMpSysprocSafeToExecute(long txnId);

    public void startSampler();

    public VoltDB.Configuration getConfig();
    public CatalogContext getCatalogContext();
    public String getBuildString();
    public String getVersionString();
    /** Can this version of VoltDB run with the version string given? */
    public boolean isCompatibleVersionString(String versionString);
    /** Version string that isn't overriden for test used to find native lib */
    public String getEELibraryVersionString();
    public HostMessenger getHostMessenger();
    public ClientInterface getClientInterface();
    public OpsAgent getOpsAgent(OpsSelector selector);
    // Keep this method to centralize the cast to StatsAgent for
    // existing code
    public StatsAgent getStatsAgent();
    public MemoryStats getMemoryStatsSource();
    public BackendTarget getBackendTargetType();
    public String getLocalMetadata();
    public SiteTracker getSiteTrackerForSnapshot();

    public void loadLegacyPathProperties(DeploymentType deployment) throws IOException;

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
     * @param diffCommands The commands to update the current catalog to the new one.
     * @param newCatalogBytes The catalog bytes.
     * @param catalogBytesHash  The SHA-1 hash of the catalog bytes
     * @param expectedCatalogVersion The version of the catalog the commands are targeted for.
     * @param currentTxnId
     * @param currentTxnTimestamp
     * @param currentTxnId  The transaction ID at which this method is called
     * @param deploymentBytes  The deployment file bytes
     * @param deploymentHash The SHA-1 hash of the deployment file
     */
    public Pair<CatalogContext, CatalogSpecificPlanner> catalogUpdate(
            String diffCommands,
            byte[] newCatalogBytes,
            byte[] catalogBytesHash,
            int expectedCatalogVersion,
            long currentTxnId,
            long currentTxnTimestamp,
            byte[] deploymentBytes,
            byte[] deploymentHash);

    /**
     * Updates the cluster setting of this VoltDB
     * @param settings the {@link ClusterSettings} update candidate
     * @param expectedVersionId version of the current instance (same as the Zookeeper node)
     * @return a {@link Pair} of {@link CatalogContext} and {@link CatalogSpecificPlanner}
     */
    public Pair<CatalogContext, CatalogSpecificPlanner> settingsUpdate(ClusterSettings settings, int expectedVersionId);

   /**
     * Tells if the VoltDB is running. m_isRunning needs to be set to true
     * when the run() method is called, and set to false when shutting down.
     *
     * @return true if the VoltDB is running.
     */
    public boolean isRunning();

    /**
     * Halt a node used by @StopNode
     */
    public void halt();

    /**
     * @return The number of milliseconds the cluster has been up
     */
    public long getClusterUptime();

    /**
     * @return The time the cluster's Create start action
     */
    public long getClusterCreateTime();

    /**
     * Set the time at which the cluster was created. This method is used when
     * in the Recover action and @SnapshotRestore paths to assign the cluster
     * create time that was preserved in the snapshot.
     */
    public void setClusterCreateTime(long clusterCreateTime);

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

    public ProducerDRGateway getNodeDRGateway();

    public ConsumerDRGateway getConsumerDRGateway();

    public void setDurabilityUniqueIdListener(Integer partition, DurableUniqueIdListener listener);

    public void onSyncSnapshotCompletion();

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

    public ScheduledExecutorService getSES(boolean priority);

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
     * @return License API based on edition.
     */
     public LicenseApi getLicenseApi();
     //Return JSON string represenation of license information.
     public String getLicenseInformation();


    public <T> ListenableFuture<T> submitSnapshotIOWork(Callable<T> work);
}
