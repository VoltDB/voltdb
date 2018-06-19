/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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


import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltcore.zk.ZKUtil;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Permission;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Iv2Trace;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.settings.NodeSettings;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public final class InvocationDispatcher {

    private static final VoltLogger log = new VoltLogger(InvocationDispatcher.class.getName());
    private static final VoltLogger authLog = new VoltLogger("AUTH");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    public enum OverrideCheck {
        NONE(false, false, false),
        INVOCATION(false, false, true)
        ;

        final boolean skipAdmimCheck;
        final boolean skipPermissionCheck;
        final boolean skipInvocationCheck;

        OverrideCheck(boolean skipAdminCheck, boolean skipPermissionCheck, boolean skipInvocationCheck) {
            this.skipAdmimCheck = skipAdminCheck;
            this.skipPermissionCheck = skipPermissionCheck;
            this.skipInvocationCheck = skipInvocationCheck;
        }
    }

    /**
     * This reference is shared with the one in {@link ClientInterface}
     */
    private final AtomicReference<CatalogContext> m_catalogContext;
    private final long m_siteId;
    private final Mailbox m_mailbox;
    //This validator will verify params or per procedure invocation validation.
    private final InvocationValidator m_invocationValidator;
    //This validator will check permissions in AUTH system.
    private final PermissionValidator m_permissionValidator = new PermissionValidator();
    private final Cartographer m_cartographer;
    private final ConcurrentMap<Long, ClientInterfaceHandleManager> m_cihm;
    private final AtomicReference<Map<Integer,Long>> m_localReplicas = new AtomicReference<>(ImmutableMap.of());
    private final SnapshotDaemon m_snapshotDaemon;
    private final AtomicBoolean m_isInitialRestore = new AtomicBoolean(true);
    private final VoltTable statusTable = new VoltTable(new VoltTable.ColumnInfo("STATUS", VoltType.BIGINT));

    private final NTProcedureService m_NTProcedureService;

    // Next partition to service adhoc replicated table reads
    private static int m_nextPartition = -1;
    // Number of partitions, will NOT change when new node joins cluster
    private static int m_partitionCount;
    // the partition id list, which does not assume starting from 0
    private static ArrayList<Integer> m_partitionIds;

    public final static class Builder {

        ClientInterface m_clientInterface;
        Cartographer m_cartographer;
        AtomicReference<CatalogContext> m_catalogContext;
        ConcurrentMap<Long, ClientInterfaceHandleManager> m_cihm;
        Mailbox m_mailbox;
        ReplicationRole m_replicationRole;
        SnapshotDaemon m_snapshotDaemon;
        long m_siteId;

        public Builder clientInterface(ClientInterface clientInterface) {
            m_clientInterface = checkNotNull(clientInterface, "given client interface is null");
            return this;
        }

        public Builder cartographer(Cartographer cartographer) {
            m_cartographer = checkNotNull(cartographer, "given cartographer is null");
            return this;
        }

        public Builder catalogContext(AtomicReference<CatalogContext> catalogContext) {
            m_catalogContext = checkNotNull(catalogContext, "given catalog context is null");
            return this;
        }

        public Builder clientInterfaceHandleManagerMap(ConcurrentMap<Long, ClientInterfaceHandleManager> cihm) {
            m_cihm = checkNotNull(cihm, "given client interface handler manager lookup map is null");
            return this;
        }

        public Builder mailbox(Mailbox mailbox) {
            m_mailbox = checkNotNull(mailbox, "given mailbox is null");
            return this;
        }

        public Builder replicationRole(ReplicationRole replicationRole) {
            m_replicationRole = checkNotNull(replicationRole, "given replication role is null");
            return this;
        }

        public Builder snapshotDaemon(SnapshotDaemon snapshotDaemon) {
            m_snapshotDaemon = checkNotNull(snapshotDaemon,"given snapshot daemon is null");
            return this;
        }

        public Builder siteId(long siteId) {
            m_siteId = siteId;
            return this;
        }

        public InvocationDispatcher build() {
            return new InvocationDispatcher(
                    m_clientInterface,
                    m_cartographer,
                    m_catalogContext,
                    m_cihm,
                    m_mailbox,
                    m_snapshotDaemon,
                    m_replicationRole,
                    m_siteId
                    );
        }
    }

    public static final Builder builder() {
        return new Builder();
    }

    private InvocationDispatcher(
            ClientInterface clientInterface,
            Cartographer cartographer,
            AtomicReference<CatalogContext> catalogContext,
            ConcurrentMap<Long, ClientInterfaceHandleManager> cihm,
            Mailbox mailbox,
            SnapshotDaemon snapshotDaemon,
            ReplicationRole replicationRole,
            long siteId)
    {
        m_siteId = siteId;
        m_mailbox = checkNotNull(mailbox, "given mailbox is null");
        m_catalogContext = checkNotNull(catalogContext, "given catalog context is null");
        m_cihm = checkNotNull(cihm, "given client interface handler manager lookup map is null");
        m_invocationValidator = new InvocationValidator(
                checkNotNull(replicationRole, "given replication role is null")
                );
        m_cartographer = checkNotNull(cartographer, "given cartographer is null");
        m_snapshotDaemon = checkNotNull(snapshotDaemon,"given snapshot daemon is null");

        m_NTProcedureService = new NTProcedureService(clientInterface, this, m_mailbox);
        statusTable.addRow(0);

        // this kicks off the initial NT procedures being loaded
        notifyNTProcedureServiceOfCatalogUpdate();

        // update the partition count and partition keys for routing purpose
        updatePartitionInformation();
    }

    /**
     * Tells NTProcedureService to pause before stats get smashed during UAC
     */
    void notifyNTProcedureServiceOfPreCatalogUpdate() {
        m_NTProcedureService.preUpdate();
    }

    /**
     * Tells NTProcedureService to reload NT procedures
     */
    void notifyNTProcedureServiceOfCatalogUpdate() {
        m_NTProcedureService.update(m_catalogContext.get());
    }

    LightweightNTClientResponseAdapter getInternelAdapterNT () {
        return m_NTProcedureService.m_internalNTClientAdapter;
    }

    static void updatePartitionInformation() {
        m_partitionIds = new ArrayList<>();

        VoltTable partitionKeys = TheHashinator.getPartitionKeys(VoltType.INTEGER);
        ByteBuffer buf = ByteBuffer.allocate(partitionKeys.getSerializedSize());
        partitionKeys.flattenToBuffer(buf);
        buf.flip();
        VoltTable keyCopy = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf);
        keyCopy.resetRowPosition();
        while (keyCopy.advanceRow()) {
            if (MpInitiator.MP_INIT_PID != keyCopy.getLong("PARTITION_ID")) {
                m_partitionIds.add((int)(keyCopy.getLong("PARTITION_ID")));
            }
        }
        m_partitionCount = m_partitionIds.size();
    }

    /*
     * This does a ZK lookup which apparently is full of fail
     * if you run TestRejoinEndToEnd. Kind of lame, but initializing this data
     * immediately is not critical, request routing works without it.
     *
     * Populate the map in the background and it will be used to route
     * requests to local replicas once the info is available
     */
    public Future<?> asynchronouslyDetermineLocalReplicas() {
        return VoltDB.instance().getSES(false).submit(new Runnable() {

            @Override
            public void run() {
                /*
                 * Assemble a map of all local replicas that will be used to determine
                 * if single part reads can be delivered and executed at local replicas
                 */
                final int thisHostId = CoreUtils.getHostIdFromHSId(m_mailbox.getHSId());
                ImmutableMap.Builder<Integer, Long> localReplicas = ImmutableMap.builder();
                for (int partition : m_cartographer.getPartitions()) {
                    for (Long replica : m_cartographer.getReplicasForPartition(partition)) {
                        if (CoreUtils.getHostIdFromHSId(replica) == thisHostId) {
                            localReplicas.put(partition, replica);
                        }
                    }
                }
                m_localReplicas.set(localReplicas.build());
            }

        });
    }

    public final ClientResponseImpl dispatch(
            StoredProcedureInvocation task,
            InvocationClientHandler handler,
            Connection ccxn,
            AuthUser user,
            OverrideCheck bypass,
            boolean ntPriority)
    {
        final long nowNanos = System.nanoTime();
                // Deserialize the client's request and map to a catalog stored procedure
        final CatalogContext catalogContext = m_catalogContext.get();

        String clientInfo = ccxn.getHostnameAndIPAndPort();  // Storing the client's ip information

        final String procName = task.getProcName();
        final String threadName = Thread.currentThread().getName(); // Thread name has to be materialized here
        final StoredProcedureInvocation finalTask = task;
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.meta("process_name", "name", CoreUtils.getHostnameOrAddress()))
                    .add(() -> VoltTrace.meta("thread_name", "name", threadName))
                    .add(() -> VoltTrace.meta("thread_sort_index", "sort_index", Integer.toString(1)))
                    .add(() -> VoltTrace.beginAsync("recvtxn", finalTask.getClientHandle(),
                                                    "name", procName,
                                                    "clientHandle", Long.toString(finalTask.getClientHandle())));
        }

        Procedure catProc = getProcedureFromName(task.getProcName(), catalogContext);

        if (catProc == null) {
            String errorMessage = "Procedure " + procName + " was not found";
            RateLimitedLogger.tryLogForMessage(EstTime.currentTimeMillis(),
                            60, TimeUnit.SECONDS, authLog, Level.WARN,
                            errorMessage + ". This message is rate limited to once every 60 seconds."
                            );
            return unexpectedFailureResponse(errorMessage, task.clientHandle);
        }

        ClientResponseImpl error = null;

        // Check for pause mode restrictions before proceeding any further
        if ((error = allowPauseModeExecution(handler, catProc, task)) != null) {
            if (bypass == null || !bypass.skipAdmimCheck) {
                return error;
            }
         }
        //Check permissions
        if ((error = m_permissionValidator.shouldAccept(procName, user, task, catProc)) != null) {
            if (bypass == null || !bypass.skipPermissionCheck) {
                return error;
            }
        }
        //Check param deserialization policy for sysprocs
        if ((error = m_invocationValidator.shouldAccept(procName, user, task, catProc)) != null) {
            if (bypass == null || !bypass.skipInvocationCheck) {
                return error;
            }
        }
        //Check individual query timeout value settings with privilege
        int batchTimeout = task.getBatchTimeout();
        if (BatchTimeoutOverrideType.isUserSetTimeout(batchTimeout)) {
            if (! user.hasPermission(Permission.ADMIN)) {
                int systemTimeout = catalogContext.cluster.getDeployment().
                        get("deployment").getSystemsettings().get("systemsettings").getQuerytimeout();
                if (systemTimeout != ExecutionEngine.NO_BATCH_TIMEOUT_VALUE &&
                        (batchTimeout > systemTimeout || batchTimeout == ExecutionEngine.NO_BATCH_TIMEOUT_VALUE)) {
                    String errorMessage = "The attempted individual query timeout value " + batchTimeout +
                            " milliseconds override was ignored because the connection lacks ADMIN privileges.";
                    RateLimitedLogger.tryLogForMessage(EstTime.currentTimeMillis(),
                            60, TimeUnit.SECONDS,
                            log, Level.INFO,
                            errorMessage + " This message is rate limited to once every 60 seconds.");

                    task.setBatchTimeout(systemTimeout);
                }
            }
        }

        // handle non-transactional procedures (INCLUDING NT SYSPROCS)
        // note that we also need to check for java for now as transactional flag is
        // only 100% when we're talking Java
        if ((catProc.getTransactional() == false) && catProc.getHasjava()) {
            return dispatchNTProcedure(handler, task, user, ccxn, nowNanos, ntPriority);
        }

        // check for allPartition invocation and provide a nice error if it's misused
        if (task.getAllPartition()) {
            // must be single partition and must be partitioned on parameter 0
            if (!catProc.getSinglepartition() || (catProc.getPartitionparameter() != 0) || catProc.getSystemproc()) {
                return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                        new VoltTable[0], "Invalid procedure for all-partition execution. " +
                                 "Targeted procedure must be partitioned, must be partitioned on the first parameter, " +
                                 "and must not be a system procedure.",
                        task.clientHandle);
            }
        }

        if (catProc.getSystemproc()) {
            // COMMUNITY SYSPROC SPECIAL HANDLING

            // ping just responds as fast as possible to show the connection is alive
            // nb: ping is not a real procedure, so this is checked before other "sysprocs"
            if ("@Ping".equals(procName)) {
                return new ClientResponseImpl(ClientResponseImpl.SUCCESS, new VoltTable[]{statusTable}, "SUCCESS", task.clientHandle);
            }
            else if ("@GetPartitionKeys".equals(procName)) {
                return dispatchGetPartitionKeys(task);
            }
            else if ("@Subscribe".equals(procName)) {
                return dispatchSubscribe( handler, task);
            }
            else if ("@Statistics".equals(procName)) {
                return dispatchStatistics(OpsSelector.STATISTICS, task, ccxn);
            }
            else if ("@SystemCatalog".equals(procName)) {
                return dispatchStatistics(OpsSelector.SYSTEMCATALOG, task, ccxn);
            }
            else if ("@SystemInformation".equals(procName)) {
                return dispatchStatistics(OpsSelector.SYSTEMINFORMATION, task, ccxn);
            }
            else if ("@Trace".equals(procName)) {
                return dispatchStatistics(OpsSelector.TRACE, task, ccxn);
            }
            else if ("@StopNode".equals(procName)) {
                CoreUtils.logProcedureInvocation(hostLog, user.m_name, clientInfo, procName);
                return dispatchStopNode(task);
            }
            else if ("@LoadSinglepartitionTable".equals(procName)) {
                // FUTURE: When we get rid of the legacy hashinator, this should go away
                return dispatchLoadSinglepartitionTable(catProc, task, handler, ccxn);
            }
            else if ("@SnapshotSave".equals(procName)) {
                m_snapshotDaemon.requestUserSnapshot(task, ccxn);
                return null;
            }
            else if ("@SnapshotStatus".equals(procName)) {
                // SnapshotStatus is really through @Statistics now, but preserve the
                // legacy calling mechanism
                Object[] params = new Object[] { "SNAPSHOTSTATUS" };
                task.setParams(params);
                return dispatchStatistics(OpsSelector.STATISTICS, task, ccxn);
            }
            else if ("@SnapshotScan".equals(procName)) {
                return dispatchStatistics(OpsSelector.SNAPSHOTSCAN, task, ccxn);
            }
            else if ("@SnapshotDelete".equals(procName)) {
                return dispatchStatistics(OpsSelector.SNAPSHOTDELETE, task, ccxn);
            }
            else if ("@SnapshotRestore".equals(procName)) {
                ClientResponseImpl retval = SnapshotUtil.transformRestoreParamsToJSON(task);
                if (retval != null) {
                    return retval;
                }
                if (m_isInitialRestore.compareAndSet(true, false) && isSchemaEmpty()) {
                    m_NTProcedureService.isRestoring = true;
                    return useSnapshotCatalogToRestoreSnapshotSchema(task, handler, ccxn, user, bypass);
                }
            }
            else if ("@Shutdown".equals(procName)) {
                if (task.getParams().size() == 1) {
                    return takeShutdownSaveSnapshot(task, handler, ccxn, user, bypass);
                }
            }
            else if ("@UpdateLogging".equals(procName)) {
                task = appendAuditParams(task, ccxn, user);
            }

            // ERROR MESSAGE FOR PRO SYSPROC USE IN COMMUNITY

            if (!MiscUtils.isPro()) {
                SystemProcedureCatalog.Config sysProcConfig = SystemProcedureCatalog.listing.get(procName);
                if ((sysProcConfig != null) && (sysProcConfig.commercial)) {
                    return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                            new VoltTable[0],
                            procName + " is available in the Enterprise Edition of VoltDB only.",
                            task.clientHandle);
                }
            }

            // Verify that admin mode sysprocs are called from a client on the
            // admin port, otherwise return a failure
            if (    "@Pause".equals(procName)
                 || "@Resume".equals(procName)
                 || "@PrepareShutdown".equals(procName))
            {
                if (handler.isAdmin() == false) {
                    return unexpectedFailureResponse(
                            procName + " is not available to this client",
                            task.clientHandle);
                }
                // Log the invocation with user name and ip information
                CoreUtils.logProcedureInvocation(hostLog, user.m_name, clientInfo, procName);
            }
        }
        // If you're going to copy and paste something, CnP the pattern
        // up above.  -rtb.

        int[] partitions = null;
        try {
            partitions = getPartitionsForProcedure(catProc, task);
        } catch (Exception e) {
            // unable to hash to a site, return an error
            return getMispartitionedErrorResponse(task, catProc, e);
        }
        boolean success = createTransaction(handler.connectionId(),
                        task,
                        catProc.getReadonly(),
                        catProc.getSinglepartition(),
                        catProc.getEverysite(),
                        partitions,
                        task.getSerializedSize(),
                        nowNanos);
        if (!success) {
            // when VoltDB.crash... is called, we close off the client interface
            // and it might not be possible to create new transactions.
            // Return an error.
            return new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0],
                    "VoltDB failed to create the transaction internally.  It is possible this "
                    + "was caused by a node failure or intentional shutdown. If the cluster recovers, "
                    + "it should be safe to resend the work, as the work was never started.",
                    task.clientHandle);
        }

        return null;
    }

    private final boolean isSchemaEmpty() {
        return m_catalogContext.get().database.getTables().size() == 0;
    }

    public final static Procedure getProcedureFromName(String procName, CatalogContext catalogContext) {
        Procedure catProc = catalogContext.procedures.get(procName);
        if (catProc == null) {
            catProc = catalogContext.m_defaultProcs.checkForDefaultProcedure(procName);
        }

        if (catProc == null) {
            Config sysProc = SystemProcedureCatalog.listing.get(procName);
            if (sysProc != null) {
                catProc = sysProc.asCatalogProcedure();
            }
        }
        return catProc;
    }

    public final static String SHUTDOWN_MSG = "Server is shutting down.";

    private final static ClientResponseImpl allowPauseModeExecution(
            InvocationClientHandler handler,
            Procedure procedure,
            StoredProcedureInvocation task)
    {
        final VoltDBInterface voltdb = VoltDB.instance();

        if (voltdb.getMode() == OperationMode.SHUTTINGDOWN) {
            return serverUnavailableResponse(
                    SHUTDOWN_MSG,
                    task.clientHandle);
        }

        if (voltdb.isPreparingShuttingdown()) {
            if (procedure.getAllowedinshutdown()) return null;

            return serverUnavailableResponse(
                    SHUTDOWN_MSG,
                    task.clientHandle);
        }

        if (voltdb.getMode() != OperationMode.PAUSED || handler.isAdmin()) {
            return null;
        }

        // If we got here, instance is paused and handler is not admin.
        final String procName = task.getProcName();
        if (procedure.getSystemproc() &&
                ("@AdHoc".equals(procName) || "@AdHocSpForTest".equals(procName))) {
            // AdHoc is handled after it is planned and we figure out if it is read-only or not.
            return null;
        } else if (!procedure.getReadonly()) {
            return serverUnavailableResponse(
                    "Server is paused and is available in read-only mode - please try again later.",
                    task.clientHandle);

        }
        return null;
    }

    private final static ClientResponseImpl dispatchGetPartitionKeys(StoredProcedureInvocation task) {
        Object params[] = task.getParams().toArray();
        String typeString = "the type of partition key to return and can be one of " +
                            "INTEGER, STRING or VARCHAR (equivalent), or VARBINARY";
        if (params.length != 1 || params[0] == null) {
            return gracefulFailureResponse(
                    "GetPartitionKeys must have one string parameter specifying " + typeString,
                    task.clientHandle);
        }
        if (!(params[0] instanceof String)) {
            return gracefulFailureResponse(
                    "GetPartitionKeys must have one string parameter specifying " + typeString +
                    " provided type was " + params[0].getClass().getName(), task.clientHandle);
        }
        VoltType voltType = null;
        String typeStr = ((String)params[0]).trim().toUpperCase();
        if ("INTEGER".equals(typeStr)) {
            voltType = VoltType.INTEGER;
        } else if ("STRING".equals(typeStr) || "VARCHAR".equals(typeStr)) {
            voltType = VoltType.STRING;
        } else if ("VARBINARY".equals(typeStr)) {
            voltType = VoltType.VARBINARY;
        } else {
            return gracefulFailureResponse(
                    "Type " + typeStr + " is not a supported type of partition key, " + typeString,
                    task.clientHandle);
        }
        VoltTable partitionKeys = TheHashinator.getPartitionKeys(voltType);
        if (partitionKeys == null) {
            return gracefulFailureResponse(
                    "Type " + typeStr + " is not a supported type of partition key, " + typeString,
                    task.clientHandle);
        }
        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { partitionKeys }, null, task.clientHandle);
    }

    private final ClientResponseImpl dispatchSubscribe(InvocationClientHandler handler, StoredProcedureInvocation task) {
        final ParameterSet ps = task.getParams();
        final Object params[] = ps.toArray();
        String err = null;
        final ClientInterfaceHandleManager cihm = m_cihm.get(handler.connectionId());
        //Not sure if it can actually be null, not really important if it is
        if (cihm == null) {
            return null;
        }
        for (int ii = 0; ii < params.length; ii++) {
            final Object param = params[ii];
            if (param == null) {
                err = "Parameter index " + ii + " was null"; break;
            }
            if (!(param instanceof String)) {
                err = "Parameter index " + ii + " was not a String"; break;
            }

            if ("TOPOLOGY".equals(param)) {
                cihm.setWantsTopologyUpdates(true);
            } else {
                err = "Parameter \"" + param + "\" is not recognized/supported"; break;
            }
        }
        return new ClientResponseImpl(
                       err == null ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                       new VoltTable[] { },
                       err,
                       task.clientHandle);
    }

    final static ClientResponseImpl dispatchStatistics(OpsSelector selector, StoredProcedureInvocation task, Connection ccxn) {
        try {
            OpsAgent agent = VoltDB.instance().getOpsAgent(selector);
            if (agent != null) {
                agent.performOpsAction(ccxn, task.clientHandle, selector, task.getParams());
            }
            else {
                return errorResponse(ccxn, task.clientHandle, ClientResponse.GRACEFUL_FAILURE,
                        "Unknown OPS selector", null, true);
            }

            return null;
        } catch (Exception e) {
            return errorResponse( ccxn, task.clientHandle, ClientResponse.UNEXPECTED_FAILURE, null, e, true);
        }
    }

    private ClientResponseImpl dispatchStopNode(StoredProcedureInvocation task) {
        Object params[] = task.getParams().toArray();
        if (params.length != 1 || params[0] == null) {
            return gracefulFailureResponse(
                    "@StopNode must provide hostId",
                    task.clientHandle);
        }
        if (!(params[0] instanceof Integer)) {
            return gracefulFailureResponse(
                    "@StopNode must have one Integer parameter specified. Provided type was " + params[0].getClass().getName(),
                    task.clientHandle);
        }
        int ihid = (Integer) params[0];
        final HostMessenger hostMessenger = VoltDB.instance().getHostMessenger();
        Set<Integer> liveHids = hostMessenger.getLiveHostIds();
        if (!liveHids.contains(ihid)) {
            return gracefulFailureResponse(
                    "Invalid Host Id or Host Id not member of cluster: " + ihid,
                    task.clientHandle);
        }
        String reason = m_cartographer.stopNodeIfClusterIsSafe(liveHids, ihid);
        if (reason != null) {
            hostLog.info("It's unsafe to shutdown node " + ihid
                    + ". Cannot stop the requested node. " + reason
                    + ". Use shutdown to stop the cluster.");
            return gracefulFailureResponse(
                    "It's unsafe to shutdown node " + ihid
                  + ". Cannot stop the requested node. " + reason
                  + ". Use shutdown to stop the cluster.", task.clientHandle);
        }

        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[0], "SUCCESS", task.clientHandle);
    }

    public final ClientResponseImpl dispatchNTProcedure(InvocationClientHandler handler,
                                                        StoredProcedureInvocation task,
                                                        AuthUser user,
                                                        Connection ccxn,
                                                        long nowNanos,
                                                        boolean ntPriority)
    {
        // get the CIHM
        long connectionId = handler.connectionId();
        final ClientInterfaceHandleManager cihm = m_cihm.get(connectionId);
        if (cihm == null) {
            hostLog.rateLimitedLog(60, Level.WARN, null,
                    "Dispatch Non-Transactional Procedure request rejected. "
                    + "This is likely due to VoltDB ceasing client communication as it "
                    + "shuts down.");

            // when VoltDB.crash... is called, we close off the client interface
            // and it might not be possible to create new transactions.
            // Return an error.
            return new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0],
                    "VoltDB failed to create the transaction internally.  It is possible this "
                    + "was caused by a node failure or intentional shutdown. If the cluster recovers, "
                    + "it should be safe to resend the work, as the work was never started.",
                    task.clientHandle);
        }

        // This handle is needed for backpressure. It identifies this transaction to the ACG and
        // increments backpressure. When the response is sent (by sending an InitiateResponseMessage
        // to the CI mailbox, the backpressure associated with this handle will go away.
        // Sadly, many of the value's here are junk.
        long handle = cihm.getHandle(true,
                                     ClientInterface.NTPROC_JUNK_ID,
                                     task.clientHandle,
                                     task.getSerializedSize(),
                                     nowNanos,
                                     task.getProcName(),
                                     ClientInterface.NTPROC_JUNK_ID,
                                     false);

        // note, once we get the handle above, any response to the client MUST be done
        // by sending an InitiateResponseMessage to the CI mailbox. Writing bytes to the wire, like we
        // do at the top of this method won't release any backpressure accounting.

        // actually kick off the NT proc
        m_NTProcedureService.callProcedureNT(handle,
                                             user,
                                             ccxn,
                                             handler.isAdmin(),
                                             ntPriority,
                                             task);
        return null;
    }

    private StoredProcedureInvocation appendAuditParams(StoredProcedureInvocation task,
            Connection ccxn, AuthSystem.AuthUser user) {
        String username = user.m_name;
        if (username == null) {
            username = "An anonymous user";
        }
        String remoteHost = ccxn.getRemoteSocketAddress().toString();
        String xml = (String)task.getParams().toArray()[0];
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName(task.getProcName());
        spi.setParams(username, remoteHost, xml);
        spi.setClientHandle(task.getClientHandle());
        spi.setBatchTimeout(task.getBatchTimeout());
        spi.type = task.getType();
        spi.setAllPartition(task.getAllPartition());
        return spi;
    }

   /**
     * Send a command log replay sentinel to the given partition.
     * @param txnId
     * @param partitionId
     */
    public final void sendSentinel(long txnId, int partitionId) {
        final long initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(partitionId);
        sendSentinel(txnId, initiatorHSId, -1, -1, true);
    }

    private final void sendSentinel(long txnId, long initiatorHSId, long ciHandle,
                              long connectionId, boolean forReplay) {
        //The only field that is relevant is txnid, and forReplay.
        MultiPartitionParticipantMessage mppm =
                new MultiPartitionParticipantMessage(
                        m_siteId,
                        initiatorHSId,
                        txnId,
                        ciHandle,
                        connectionId,
                        false,  // isReadOnly
                        forReplay);  // isForReplay
        m_mailbox.send(initiatorHSId, mppm);
    }

    /**
     * Coward way out of the legacy hashinator hell. LoadSinglepartitionTable gets the
     * partitioning parameter as a byte array. Legacy hashinator hashes numbers and byte arrays
     * differently, so have to convert it back to long if it's a number. UGLY!!!
     */
    private final ClientResponseImpl dispatchLoadSinglepartitionTable(Procedure catProc,
                                                        StoredProcedureInvocation task,
                                                        InvocationClientHandler handler,
                                                        Connection ccxn)
    {
        int partition = -1;
        try {
            CatalogMap<Table> tables = m_catalogContext.get().database.getTables();
            int partitionParamType = getLoadSinglePartitionTablePartitionParamType(tables, task);
            byte[] valueToHash = (byte[])task.getParameterAtIndex(0);
            partition = TheHashinator.getPartitionForParameter(partitionParamType, valueToHash);
        }
        catch (Exception e) {
            authLog.warn(e.getMessage());
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                          new VoltTable[0], e.getMessage(), task.clientHandle);
        }
        assert(partition != -1);
        createTransaction(handler.connectionId(),
                          task,
                          catProc.getReadonly(),
                          catProc.getSinglepartition(),
                          catProc.getEverysite(),
                          new int[] { partition },
                          task.getSerializedSize(),
                          System.nanoTime());
        return null;
    }

    /**
     * XXX: This should go away when we get rid of the legacy hashinator.
     */
    private final static int getLoadSinglePartitionTablePartitionParamType(CatalogMap<Table> tables,
                                                                     StoredProcedureInvocation spi)
        throws Exception
    {
        String tableName = (String) spi.getParameterAtIndex(1);

        // get the table from the catalog
        Table catTable = tables.getIgnoreCase(tableName);
        if (catTable == null) {
            throw new Exception(String .format("Unable to find target table \"%s\" for LoadSinglepartitionTable.",
                                               tableName));
        }

        Column pCol = catTable.getPartitioncolumn();
        return pCol.getType();
    }

    public void setReplicationRole(ReplicationRole role) {
        m_invocationValidator.setReplicationRole(role);
    }

    private final static void transmitResponseMessage(ClientResponse r, Connection ccxn, long handle) {
        ClientResponseImpl response = ClientResponseImpl.class.cast(r);
        response.setClientHandle(handle);
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf).flip();
        ccxn.writeStream().enqueue(buf);
    }

    private final ClientResponseImpl takeShutdownSaveSnapshot(
            final StoredProcedureInvocation task,
            final InvocationClientHandler handler, final Connection ccxn,
            final AuthUser user, OverrideCheck bypass
            )
    {
        Object p0 = task.getParams().getParam(0);
        final long zkTxnId;
        if (p0 instanceof Long) {
            zkTxnId = ((Long)p0).longValue();
        } else if (p0 instanceof String) {
            try {
                zkTxnId = Long.parseLong((String)p0);
            } catch (NumberFormatException e) {
                return gracefulFailureResponse(
                        "Incorrect argument type",
                        task.clientHandle);
            }
        } else {
            return gracefulFailureResponse(
                    "Incorrect argument type",
                    task.clientHandle);

        }
        VoltDBInterface voltdb = VoltDB.instance();

        if (!voltdb.isPreparingShuttingdown()) {
            log.warn("Ignoring shutdown save snapshot request as VoltDB is not shutting down");
            return unexpectedFailureResponse(
                    "Ignoring shutdown save snapshot request as VoltDB is not shutting down",
                    task.clientHandle);
        }
        final ZooKeeper zk = voltdb.getHostMessenger().getZK();
        // network threads are blocked from making zookeeper calls
        Future<Long> fut = voltdb.getSES(true).submit(new Callable<Long>() {
            @Override
            public  Long call() {
                try {
                    Stat stat = zk.exists(VoltZK.operationMode, false);
                    if (stat == null) {
                        VoltDB.crashLocalVoltDB("cluster operation mode zookeeper node does not exist");
                        return Long.MIN_VALUE;
                    }
                    return stat.getMzxid();
                } catch (KeeperException | InterruptedException e) {
                    VoltDB.crashLocalVoltDB("Failed to stat the cluster operation zookeeper node", true, e);
                    return Long.MIN_VALUE;
                }
            }
        });
        try {
            if (fut.get().longValue() != zkTxnId) {
                return unexpectedFailureResponse(
                        "Internal error: cannot write a startup snapshot because the " +
                        "current system state is not consistent with an orderly shutdown. " +
                        "Please try \"voltadmin shutdown --save\" again.",
                        task.clientHandle);
            }
        } catch (InterruptedException | ExecutionException e1) {
            VoltDB.crashLocalVoltDB("Failed to stat the cluster operation zookeeper node", true, e1);
            return null;
        }

        NodeSettings paths = m_catalogContext.get().getNodeSettings();
        String data;

        try {
            data = new JSONStringer()
                    .object()
                    .keySymbolValuePair(SnapshotUtil.JSON_TERMINUS, zkTxnId)
                    .endObject()
                    .toString();
        } catch (JSONException e) {
            VoltDB.crashLocalVoltDB("Failed to create startup snapshot save command", true, e);
            return null;
        }
        log.info("Saving startup snapshot");
        consoleLog.info("Taking snapshot to save database contents");


        final SimpleClientResponseAdapter alternateAdapter = new SimpleClientResponseAdapter(
                ClientInterface.SHUTDONW_SAVE_CID, "Blocking Startup Snapshot Save"
                );
        final InvocationClientHandler alternateHandler = new InvocationClientHandler() {
            @Override
            public boolean isAdmin() {
                return handler.isAdmin();
            }
            @Override
            public long connectionId() {
                return ClientInterface.SHUTDONW_SAVE_CID;
            }
        };

        final long sourceHandle = task.clientHandle;

        task.setClientHandle(alternateAdapter.registerCallback(SimpleClientResponseAdapter.NULL_CALLBACK));

        SnapshotUtil.SnapshotResponseHandler savCallback = new SnapshotUtil.SnapshotResponseHandler() {

            @Override
            public void handleResponse(ClientResponse r) {
                if (r == null) {
                    String msg = "Snapshot save failed. The database is paused and the shutdown has been cancelled";
                    transmitResponseMessage(gracefulFailureResponse(msg, sourceHandle), ccxn, sourceHandle);
                }
                if (r.getStatus() != ClientResponse.SUCCESS) {
                    String msg = "Snapshot save failed: "
                               + r.getStatusString()
                               + ". The database is paused and the shutdown has been cancelled";
                    ClientResponseImpl resp = new ClientResponseImpl(
                            ClientResponse.GRACEFUL_FAILURE,
                            r.getResults(),
                            msg,
                            sourceHandle);
                    transmitResponseMessage(resp, ccxn, sourceHandle);
                }
                consoleLog.info("Snapshot taken successfully");
                task.setParams();
                dispatch(task, alternateHandler, alternateAdapter, user, bypass, false);
            }
        };

        // network threads are blocked from making zookeeper calls
        final byte [] guardContent = data.getBytes(StandardCharsets.UTF_8);
        Future<Boolean> guardFuture = voltdb.getSES(true).submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    ZKUtil.asyncMkdirs(zk, VoltZK.shutdown_save_guard, guardContent).get();
                } catch (NodeExistsException itIsOk) {
                    return false;
                } catch (InterruptedException | KeeperException e) {
                    VoltDB.crashLocalVoltDB("Failed to create shutdown save guard zookeeper node", true, e);
                    return false;
                }
                return true;
            }
        });
        boolean created;
        try {
            created = guardFuture.get().booleanValue();
        } catch (InterruptedException | ExecutionException e) {
            VoltDB.crashLocalVoltDB("Failed to create shutdown save guard zookeeper node", true, e);
            return null;
        }
        if (!created) {
            return unexpectedFailureResponse(
                    "Internal error: detected concurrent invocations of \"voltadmin shutdown --save\"",
                    task.clientHandle);
        }

        voltdb.getClientInterface().bindAdapter(alternateAdapter, null);
        SnapshotUtil.requestSnapshot(
                sourceHandle,
                paths.resolveToAbsolutePath(paths.getSnapshoth()).toPath().toUri().toString(),
                SnapshotUtil.getShutdownSaveNonce(zkTxnId),
                true,
                SnapshotFormat.NATIVE,
                SnapshotPathType.SNAP_AUTO,
                data,
                savCallback,
                true
                );

        return null;
    }

    private final File getSnapshotCatalogFile(JSONObject snapJo) throws JSONException {
        NodeSettings paths = m_catalogContext.get().getNodeSettings();
        String catFN = snapJo.getString(SnapshotUtil.JSON_NONCE) + ".jar";
        SnapshotPathType pathType = SnapshotPathType.valueOf(
                snapJo.optString(SnapshotUtil.JSON_PATH_TYPE, SnapshotPathType.SNAP_PATH.name()));
        switch(pathType) {
        case SNAP_AUTO:
            return new File(paths.resolveToAbsolutePath(paths.getSnapshoth()), catFN);
        case SNAP_CL:
            return new File(paths.resolveToAbsolutePath(paths.getCommandLogSnapshot()), catFN);
        default:
            File snapDH = new VoltFile(snapJo.getString(SnapshotUtil.JSON_PATH));
            return new File(snapDH, catFN);
        }
    }

    private final ClientResponseImpl useSnapshotCatalogToRestoreSnapshotSchema(
            final StoredProcedureInvocation task,
            final InvocationClientHandler handler, final Connection ccxn,
            final AuthUser user, OverrideCheck bypass
            )
    {
        CatalogContext catalogContext = m_catalogContext.get();
        if (!catalogContext.cluster.getUseddlschema()) {
            return gracefulFailureResponse(
                    "Cannot restore catalog from snapshot when schema is set to catalog in the deployment.",
                    task.clientHandle);
        }
        log.info("No schema found. Restoring schema and procedures from snapshot.");
        try {
            JSONObject jsObj = new JSONObject(task.getParams().getParam(0).toString());
            final File catalogFH = getSnapshotCatalogFile(jsObj);

            final byte[] catalog;
            try {
                catalog = MiscUtils.fileToBytes(catalogFH);
            } catch (IOException e) {
                log.warn("Unable to access file " + catalogFH, e);
                return unexpectedFailureResponse(
                        "Unable to access file " + catalogFH,
                        task.clientHandle);
            }
            final String dep = new String(catalogContext.getDeploymentBytes(), StandardCharsets.UTF_8);

            final StoredProcedureInvocation catalogUpdateTask = new StoredProcedureInvocation();

            catalogUpdateTask.setProcName("@UpdateApplicationCatalog");
            catalogUpdateTask.setParams(catalog,dep);

            //A connection with positive id will be thrown into live client statistics. The connection does not support stats.
            //Thus make the connection id as a negative constant to skip the stats collection.
            final SimpleClientResponseAdapter alternateAdapter = new SimpleClientResponseAdapter(
                    ClientInterface.RESTORE_SCHEMAS_CID, "Empty database snapshot restore catalog update"
                    );
            final InvocationClientHandler alternateHandler = new InvocationClientHandler() {
                @Override
                public boolean isAdmin() {
                    return handler.isAdmin();
                }
                @Override
                public long connectionId() {
                    return ClientInterface.RESTORE_SCHEMAS_CID;
                }
            };

            final long sourceHandle = task.clientHandle;
            SimpleClientResponseAdapter.SyncCallback restoreCallback =
                    new SimpleClientResponseAdapter.SyncCallback()
                    ;
            final ListenableFuture<ClientResponse> onRestoreComplete =
                    restoreCallback.getResponseFuture()
                    ;
            onRestoreComplete.addListener(new Runnable() {
                @Override
                public void run() {
                    ClientResponse r;
                    try {
                        r = onRestoreComplete.get();
                    } catch (ExecutionException|InterruptedException e) {
                        VoltDB.crashLocalVoltDB("Should never happen", true, e);
                        return;
                    }
                    transmitResponseMessage(r, ccxn, sourceHandle);
                }
            },
            CoreUtils.SAMETHREADEXECUTOR);
            task.setClientHandle(alternateAdapter.registerCallback(restoreCallback));

            SimpleClientResponseAdapter.SyncCallback catalogUpdateCallback =
                    new SimpleClientResponseAdapter.SyncCallback()
                    ;
            final ListenableFuture<ClientResponse> onCatalogUpdateComplete =
                    catalogUpdateCallback.getResponseFuture()
                    ;
            onCatalogUpdateComplete.addListener(new Runnable() {
                @Override
                public void run() {
                    ClientResponse r;
                    try {
                        r = onCatalogUpdateComplete.get();
                    } catch (ExecutionException|InterruptedException e) {
                        VoltDB.crashLocalVoltDB("Should never happen", true, e);
                        return;
                    }
                    if (r.getStatus() != ClientResponse.SUCCESS) {
                        transmitResponseMessage(r, ccxn, sourceHandle);
                        log.error("Received error response for updating catalog " + r.getStatusString());
                        return;
                    }
                    m_catalogContext.set(VoltDB.instance().getCatalogContext());
                    dispatch(task, alternateHandler, alternateAdapter, user, bypass, false);
                }
            },
            CoreUtils.SAMETHREADEXECUTOR);
            catalogUpdateTask.setClientHandle(alternateAdapter.registerCallback(catalogUpdateCallback));

            VoltDB.instance().getClientInterface().bindAdapter(alternateAdapter, null);

            // dispatch the catalog update
            dispatchNTProcedure(alternateHandler, catalogUpdateTask, user, alternateAdapter, System.nanoTime(), false);
        }
        catch (JSONException e) {
            return unexpectedFailureResponse("Unable to parse parameters.", task.clientHandle);
        }
        return null;
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public boolean createTransaction(
            final long connectionId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int[] partitions,
            final int messageSize,
            final long nowNanos)
    {
        return createTransaction(
                connectionId,
                Iv2InitiateTaskMessage.UNUSED_MP_TXNID,
                0, //unused timestammp
                invocation,
                isReadOnly,
                isSinglePartition,
                isEveryPartition,
                partitions,
                messageSize,
                nowNanos,
                false);  // is for replay.
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public  boolean createTransaction(
            final long connectionId,
            final long txnId,
            final long uniqueId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int[] partitions,
            final int messageSize,
            long nowNanos,
            final boolean isForReplay)
    {
        assert(!isSinglePartition || (partitions.length == 1));
        final ClientInterfaceHandleManager cihm = m_cihm.get(connectionId);
        if (cihm == null) {
            hostLog.rateLimitedLog(60, Level.WARN, null,
                    "InvocationDispatcher.createTransaction request rejected. "
                    + "This is likely due to VoltDB ceasing client communication as it "
                    + "shuts down.");
            return false;
        }

        Long initiatorHSId = null;
        boolean isShortCircuitRead = false;
        /*
         * Send the read to the partition leader only
         * @MigratePartitionLeader always goes to partition leader
         */
        if (isSinglePartition && !isEveryPartition) {
            initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(partitions[0]);
        } else {
            // Multi-part transactions go to the multi-part coordinator
            initiatorHSId = m_cartographer.getHSIdForMultiPartitionInitiator();

            // Treat all MP reads as short-circuit since they can run out-of-order
            // from their arrival order due to the MP Read-only execution pool
            if (isReadOnly) {
                isShortCircuitRead = true;
            }
        }

        long handle = cihm.getHandle(isSinglePartition, isSinglePartition ? partitions[0] : -1, invocation.getClientHandle(),
                messageSize, nowNanos, invocation.getProcName(), initiatorHSId, isShortCircuitRead);

        Iv2InitiateTaskMessage workRequest =
            new Iv2InitiateTaskMessage(m_siteId,
                    initiatorHSId,
                    Iv2InitiateTaskMessage.UNUSED_TRUNC_HANDLE,
                    txnId,
                    uniqueId,
                    isReadOnly,
                    isSinglePartition,
                    (partitions == null) || (partitions.length < 2) ? null : partitions,
                    invocation,
                    handle,
                    connectionId,
                    isForReplay);

        Long finalInitiatorHSId = initiatorHSId;
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.instantAsync("inittxn",
                                                      invocation.getClientHandle(),
                                                      "clientHandle", Long.toString(invocation.getClientHandle()),
                                                      "ciHandle", Long.toString(handle),
                                                      "partitions", partitions.toString(),
                                                      "dest", CoreUtils.hsIdToString(finalInitiatorHSId)));
        }

        Iv2Trace.logCreateTransaction(workRequest);
        m_mailbox.send(initiatorHSId, workRequest);
        return true;
    }

    final static int[] getPartitionsForProcedure(Procedure procedure, StoredProcedureInvocation task) {
        final CatalogContext.ProcedurePartitionInfo ppi =
                (CatalogContext.ProcedurePartitionInfo) procedure.getAttachment();
        if (procedure.getSinglepartition()) {
            // break out the Hashinator and calculate the appropriate partition
            Object invocationParameter = task.getParameterAtIndex(ppi.index);
            if (invocationParameter == null && procedure.getReadonly()) {
                // AdHoc replicated table reads are optimized as single partition,
                // but without partition params, since replicated table reads can
                // be done on any partition, round-robin the procedure to local
                // partitions to spread the traffic.
                assert (task.getProcName().equals("@AdHoc_RO_SP")): task.getProcName();

                int partitionIdIndex = (Math.abs(++m_nextPartition)) % m_partitionCount;
                int partitionId = m_partitionIds.get(partitionIdIndex);
                return new int[] {partitionId};
            }
            return new int[] { TheHashinator.getPartitionForParameter(ppi.type, invocationParameter) };
        } else if (procedure.getPartitioncolumn2() != null) {
            // two-partition procedure
            VoltType partitionParamType1 = VoltType.get((byte)procedure.getPartitioncolumn().getType());
            VoltType partitionParamType2 = VoltType.get((byte)procedure.getPartitioncolumn2().getType());
            Object invocationParameter1 = task.getParameterAtIndex(procedure.getPartitionparameter());
            Object invocationParameter2 = task.getParameterAtIndex(procedure.getPartitionparameter2());

            int p1 = TheHashinator.getPartitionForParameter(partitionParamType1, invocationParameter1);
            int p2 = TheHashinator.getPartitionForParameter(partitionParamType2, invocationParameter2);

            return new int[] { p1, p2 };
        } else {
            // multi-partition procedure
            return new int[] { MpInitiator.MP_INIT_PID };
        }
    }

    //Generate a mispartitioned response also log the message.
    private final static ClientResponseImpl getMispartitionedErrorResponse(StoredProcedureInvocation task,
            Procedure catProc, Exception ex) {
        Object invocationParameter = null;
        try {
            invocationParameter = task.getParameterAtIndex(catProc.getPartitionparameter());
        } catch (Exception ex2) {
        }
        String exMsg = "Unknown";
        if (ex != null) {
            exMsg = ex.getMessage();
        }
        String errorMessage = "Error sending procedure " + task.getProcName()
                + " to the correct partition. Make sure parameter values are correct."
                + " Parameter value " + invocationParameter
                + ", partition column " + catProc.getPartitioncolumn().getName()
                + " type " + catProc.getPartitioncolumn().getType()
                + " Message: " + exMsg;
        authLog.warn(errorMessage);
        ClientResponseImpl clientResponse = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE,
                new VoltTable[0], errorMessage, task.clientHandle);
        return clientResponse;
    }

    private final static ClientResponseImpl errorResponse(Connection c, long handle, byte status, String reason, Exception e, boolean log) {
        String realReason = reason;
        if (e != null) {
            realReason = Throwables.getStackTraceAsString(e);
        }
        if (log) {
            hostLog.warn(realReason);
        }
        return new ClientResponseImpl(status, new VoltTable[0], realReason, handle);
    }

    private final static ClientResponseImpl unexpectedFailureResponse(String msg, long handle) {
        return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE, new VoltTable[0], msg, handle);
    }

    private final static ClientResponseImpl gracefulFailureResponse(String msg, long handle) {
        return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE, new VoltTable[0], msg, handle);
    }

    private final static ClientResponseImpl serverUnavailableResponse(String msg, long handle) {
        return new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE, new VoltTable[0], msg, handle);
    }

    /**
     * Currently passes failure notices to NTProcedureService
     */
    void handleFailedHosts(Set<Integer> failedHosts) {
        m_NTProcedureService.handleCallbacksForFailedHosts(failedHosts);
    }

    /**
     * Passes responses to NTProcedureService
     */
    public void handleAllHostNTProcedureResponse(ClientResponseImpl clientResponseData) {
        long handle = clientResponseData.getClientHandle();
        ProcedureRunnerNT runner = m_NTProcedureService.m_outstanding.get(handle);
        if (runner == null) {
            hostLog.info("Run everywhere NTProcedure early returned, probably gets timed out.");
            return;
        }
        runner.allHostNTProcedureCallback(clientResponseData);
    }

    /** test only */
    long countNTWaitingProcs() {
        return m_NTProcedureService.m_outstanding.size();
    }
}
