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


import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.ForeignHost;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.Consistency.ReadLevel;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Permission;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.AdHocPlannerWork;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWork.AsyncCompilerWorkCompletionHandler;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.CatalogChangeWork;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Iv2Trace;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.parser.SQLLexer;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListenableFutureTask;

public final class InvocationDispatcher {

    private static final VoltLogger log = new VoltLogger(InvocationDispatcher.class.getName());
    private static final VoltLogger authLog = new VoltLogger("AUTH");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     * This reference is shared with the one in {@link ClientInterface}
     */
    private final AtomicReference<CatalogContext> m_catalogContext;
    private final long m_plannerSiteId;
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
    // used to decide if we should shortcut reads
    private final Consistency.ReadLevel m_defaultConsistencyReadLevel;

    private final boolean m_isConfiguredForNonVoltDBBackend;

    public final static class Builder {

        Cartographer m_cartographer;
        AtomicReference<CatalogContext> m_catalogContext;
        ConcurrentMap<Long, ClientInterfaceHandleManager> m_cihm;
        Mailbox m_mailbox;
        ReplicationRole m_replicationRole;
        SnapshotDaemon m_snapshotDaemon;
        long m_plannerSiteId;
        long m_siteId;

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

        public Builder plannerSiteId(long plannerSiteId) {
            m_plannerSiteId = plannerSiteId;
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
                    m_cartographer,
                    m_catalogContext,
                    m_cihm,
                    m_mailbox,
                    m_snapshotDaemon,
                    m_replicationRole,
                    m_plannerSiteId,
                    m_siteId
                    );
        }
    }

    public static final Builder builder() {
        return new Builder();
    }

    private InvocationDispatcher(
            Cartographer cartographer,
            AtomicReference<CatalogContext> catalogContext,
            ConcurrentMap<Long, ClientInterfaceHandleManager> cihm,
            Mailbox mailbox,
            SnapshotDaemon snapshotDaemon,
            ReplicationRole replicationRole,
            long plannerSiteId,
            long siteId)
    {
        m_siteId = siteId;
        m_plannerSiteId = plannerSiteId;
        m_mailbox = checkNotNull(mailbox, "given mailbox is null");
        m_catalogContext = checkNotNull(catalogContext, "given catalog context is null");
        m_cihm = checkNotNull(cihm, "given client interface handler manager lookup map is null");
        m_invocationValidator = new InvocationValidator(
                checkNotNull(replicationRole, "given replication role is null")
                );
        m_cartographer = checkNotNull(cartographer, "given cartographer is null");
        BackendTarget backendTargetType = VoltDB.instance().getBackendTargetType();
        m_isConfiguredForNonVoltDBBackend = (backendTargetType == BackendTarget.HSQLDB_BACKEND ||
                                             backendTargetType == BackendTarget.POSTGRESQL_BACKEND ||
                                             backendTargetType == BackendTarget.POSTGIS_BACKEND);

        m_snapshotDaemon = checkNotNull(snapshotDaemon,"given snapshot daemon is null");

        // try to get the global default setting for read consistency, but fall back to SAFE
        m_defaultConsistencyReadLevel = VoltDB.Configuration.getDefaultReadConsistencyLevel();
    }

    /*
     * This does a ZK lookup which apparently is full of fail
     * if you run TestRejoinEndToEnd. Kind of lame, but initializing this data
     * immediately is not critical, request routing works without it.
     *
     * Populate the map in the background and it will be used to route
     * requests to local replicas once the info is available
     */
    public void asynchronouslyDetermineLocalReplicas() {
        VoltDB.instance().getSES(false).submit(new Runnable() {

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

    public final ClientResponseImpl dispatch(StoredProcedureInvocation task, InvocationClientHandler handler, Connection ccxn, AuthUser user) {
        final long nowNanos = System.nanoTime();
                // Deserialize the client's request and map to a catalog stored procedure
        final CatalogContext catalogContext = m_catalogContext.get();

        String procName = task.getProcName();
        Procedure catProc = getProcedureFromName(procName, catalogContext);

        if (catProc == null) {
            String errorMessage = "Procedure " + procName + " was not found";
            RateLimitedLogger.tryLogForMessage(EstTime.currentTimeMillis(),
                            60, TimeUnit.SECONDS, authLog, Level.WARN,
                            errorMessage + ". This message is rate limited to once every 60 seconds."
                            );
            return unexpectedFailureResponse(errorMessage, task.clientHandle);
        }
        // Check for pause mode restrictions before proceeding any further
        if (!allowPauseModeExecution(handler, catProc, task)) {
            String msg = "Server is paused and is available in read-only mode - please try again later.";
            if (VoltDB.instance().isShuttingdown()) {
                msg = "Server shutdown in progress - new transactions are not processed.";
            }
            return new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE, new VoltTable[0], msg,task.clientHandle);
         }

        ClientResponseImpl error = null;
        //Check permissions
        if ((error = m_permissionValidator.shouldAccept(procName, user, task, catProc)) != null) {
            return error;
        }
        //Check param deserialization policy for sysprocs
        if ((error = m_invocationValidator.shouldAccept(procName, user, task, catProc)) != null) {
            return error;
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

        if (catProc.getSystemproc()) {
            // COMMUNITY SYSPROC SPECIAL HANDLING

            // ping just responds as fast as possible to show the connection is alive
            // nb: ping is not a real procedure, so this is checked before other "sysprocs"
            if ("@Ping".equals(procName)) {
                return new ClientResponseImpl(ClientResponseImpl.SUCCESS, new VoltTable[0], "", task.clientHandle);
            }
            // ExecuteTask is an internal procedure, not for public use.
            else if ("@ExecuteTask".equals(procName)) {
                return unexpectedFailureResponse(
                        "@ExecuteTask is a reserved procedure only for VoltDB internal use", task.clientHandle);
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
            else if ("@GC".equals(procName)) {
                return dispatchSystemGC(handler, task);
            }
            else if ("@StopNode".equals(procName)) {
                return dispatchStopNode(task);
            }
            else if ("@Explain".equals(procName)) {
                return dispatchAdHoc(task, handler, ccxn, true, user);
            }
            else if ("@ExplainProc".equals(procName)) {
                return dispatchExplainProcedure(task, handler, ccxn, user);
            }
            else if ("@AdHoc".equals(procName)) {
                return dispatchAdHoc(task, handler, ccxn, false, user);
            }
            else if ("@AdHocSpForTest".equals(procName)) {
                return dispatchAdHocSpForTest(task, handler, ccxn, false, user);
            }
            else if (procName.equals("@LoadSinglepartitionTable")) {
                // FUTURE: When we get rid of the legacy hashinator, this should go away
                return dispatchLoadSinglepartitionTable(catProc, task, handler, ccxn);
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
            final boolean useDdlSchema = catalogContext.cluster.getUseddlschema();
            if ("@UpdateApplicationCatalog".equals(procName)) {
                return dispatchUpdateApplicationCatalog(task, handler, ccxn, user, useDdlSchema);
            }
            else if ("@UpdateClasses".equals(procName)) {
                return dispatchUpdateApplicationCatalog(task, handler, ccxn, user, useDdlSchema);
            }
            else if ("@SnapshotSave".equals(procName)) {
                m_snapshotDaemon.requestUserSnapshot(task, ccxn);
                return null;
            }
            else if ("@Promote".equals(procName)) {
                return dispatchPromote(catProc, task, handler, ccxn);
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
                    return useSnapshotCatalogToRestoreSnapshotSchema(task, handler, ccxn, user);
                }
            }
        }
        // If you're going to copy and paste something, CnP the pattern
        // up above.  -rtb.

        // Verify that admin mode sysprocs are called from a client on the
        // admin port, otherwise return a failure
        if (("@Pause".equals(procName) || "@Resume".equals(procName) || "@PrepareShutdown".equals(procName)) && !handler.isAdmin()) {
            return unexpectedFailureResponse(
                    procName + " is not available to this client",
                    task.clientHandle);
        }

        int partition = -1;
        try {
            partition = getPartitionForProcedure(catProc, task);
        } catch (Exception e) {
            // unable to hash to a site, return an error
            return getMispartitionedErrorResponse(task, catProc, e);
        }
        boolean success = createTransaction(handler.connectionId(),
                        task,
                        catProc.getReadonly(),
                        catProc.getSinglepartition(),
                        catProc.getEverysite(),
                        partition,
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
            String proc = procName;
            if ("@AdHoc".equals(procName) || "@AdHocSpForTest".equals(procName)) {
                // Map @AdHoc... to @AdHoc_RW_MP for validation. In the future if security is
                // configured differently for @AdHoc... variants this code will have to
                // change in order to use the proper variant based on whether the work
                // is single or multi partition and read-only or read-write.
                proc = "@AdHoc_RW_MP";
            }
            else if ("@UpdateClasses".equals(procName)) {
                // Icky.  Map @UpdateClasses to @UpdateApplicationCatalog.  We want the
                // permissions and replication policy for @UAC, and we'll deal with the
                // parameter validation stuff separately (the different name will
                // skip the @UAC-specific policy)
                proc = "@UpdateApplicationCatalog";
            }
            Config sysProc = SystemProcedureCatalog.listing.get(proc);
            if (sysProc != null) {
                catProc = sysProc.asCatalogProcedure();
            }
        }
        return catProc;
    }

    private final static boolean allowPauseModeExecution(InvocationClientHandler handler, Procedure procedure, StoredProcedureInvocation invocation) {
        //@Statistics and  @Shutdown are allowed in pause/shutdown mode
        if (VoltDB.instance().isShuttingdown()) {
            return procedure.getAllowedinshutdown();
        }

        if (VoltDB.instance().getMode() != OperationMode.PAUSED || handler.isAdmin()) {
            return true;
        }

        // If we got here, instance is paused and handler is not admin.
        if (procedure.getSystemproc() &&
                ("@AdHoc".equals(invocation.getProcName()) || "@AdHocSpForTest".equals(invocation.getProcName()))) {
            // AdHoc is handled after it is planned and we figure out if it is read-only or not.
            return true;
        } else {
            return procedure.getReadonly();
        }
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

    //Run System.gc() in it's own thread because it will block
    //until collection is complete and we don't want to do that from an application thread
    //because the collector is partially concurrent and we can still make progress
    private final ExecutorService m_systemGCThread =
            CoreUtils.getCachedSingleThreadExecutor("System.gc() invocation thread", 1000);

    private final ClientResponseImpl dispatchSystemGC(final InvocationClientHandler handler, final StoredProcedureInvocation task) {
        m_systemGCThread.execute(new Runnable() {
            @Override
            public void run() {
                final long start = System.nanoTime();
                System.gc();
                final long duration = System.nanoTime() - start;
                VoltTable vt = new VoltTable(
                        new ColumnInfo[] { new ColumnInfo("SYSTEM_GC_DURATION_NANOS", VoltType.BIGINT) });
                vt.addRow(duration);
                final ClientResponseImpl response = new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        new VoltTable[] { vt },
                        null,
                        task.clientHandle);
                ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                response.flattenToBuffer(buf).flip();

                ClientInterfaceHandleManager cihm = m_cihm.get(handler.connectionId());
                if (cihm == null) {
                    return;
                }
                cihm.connection.writeStream().enqueue(buf);
            }
        });
        return null;

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
        if (!m_cartographer.isClusterSafeIfNodeDies(liveHids, ihid)) {
            hostLog.info("Its unsafe to shutdown node with hostId: " + ihid
                    + " Cannot stop the requested node. Stopping individual nodes is only allowed on a K-safe cluster."
                    + " And all rejoin nodes should be completed."
                    + " Use shutdown to stop the cluster.");
            return gracefulFailureResponse(
                    "Cannot stop the requested node. Stopping individual nodes is only allowed on a K-safe cluster."
                  + " And all rejoin nodes should be completed."
                  + " Use shutdown to stop the cluster.", task.clientHandle);
        }


        int hid = hostMessenger.getHostId();
        if (hid == ihid) {
            //Killing myself no pill needs to be sent
            VoltDB.instance().halt();
        } else {
            //Send poison pill with target to kill
            hostMessenger.sendPoisonPill("@StopNode", ihid, ForeignHost.CRASH_ME);
        }
        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[0], "SUCCESS", task.clientHandle);
    }

    // Go to the catalog and fetch all the "explain plan" strings of the queries in the procedure.
    private final ClientResponseImpl dispatchExplainProcedure(StoredProcedureInvocation task, InvocationClientHandler handler,
            Connection ccxn, AuthUser user) {
        ParameterSet params = task.getParams();
        /*
         * TODO: We don't actually support multiple proc names in an ExplainProc call,
         * so I THINK that the string is always a single procname symbol and all this
         * splitting and iterating is a no-op.
         */
        //String procs = (String) params.toArray()[0];
        List<String> procNames = SQLLexer.splitStatements( (String)params.toArray()[0]);
        int size = procNames.size();
        VoltTable[] vt = new VoltTable[ size ];
        for( int i=0; i<size; i++ ) {
            String procName = procNames.get(i);

            // look in the catalog
            Procedure proc = m_catalogContext.get().procedures.get(procName);
            if (proc == null) {
                // check default procs and send them off to be explained using the regular
                // adhoc explain process
                proc = m_catalogContext.get().m_defaultProcs.checkForDefaultProcedure(procName);
                if (proc != null) {
                    String sql = m_catalogContext.get().m_defaultProcs.sqlForDefaultProc(proc);
                    dispatchAdHocCommon(task, handler, ccxn, ExplainMode.EXPLAIN_DEFAULT_PROC, sql, new Object[0], null, user);
                    return null;
                }

                return unexpectedFailureResponse("Procedure "+procName+" not in catalog", task.clientHandle);
            }

            vt[i] = new VoltTable(new VoltTable.ColumnInfo( "SQL_STATEMENT", VoltType.STRING),
                                  new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));

            for( Statement stmt : proc.getStatements() ) {
                vt[i].addRow( stmt.getSqltext(), Encoder.hexDecodeToString( stmt.getExplainplan() ) );
            }
        }

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        vt,
                        null);
        response.setClientHandle( task.clientHandle );
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        ccxn.writeStream().enqueue(buf);
        return null;
    }

    private final ClientResponseImpl dispatchAdHoc(StoredProcedureInvocation task, InvocationClientHandler handler,
            Connection ccxn, boolean isExplain, AuthSystem.AuthUser user) {
        ParameterSet params = task.getParams();
        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        Object[] userParams = null;
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }
        ExplainMode explainMode = isExplain ? ExplainMode.EXPLAIN_ADHOC : ExplainMode.NONE;
        dispatchAdHocCommon(task, handler, ccxn, explainMode, sql, userParams, null, user);
        return null;
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

    private final ClientResponseImpl dispatchAdHocSpForTest(StoredProcedureInvocation task,
            InvocationClientHandler handler, Connection ccxn, boolean isExplain, AuthSystem.AuthUser user) {
        ParameterSet params = task.getParams();
        assert(params.size() > 1);
        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        // get the partition param which must exist
        Object[] userPartitionKey = Arrays.copyOfRange(paramArray, 1, 2);
        Object[] userParams = null;
        // There's no reason (any more) that AdHocSP's can't have '?' parameters, but
        // note that the explicit partition key argument is not considered one of them.
        if (params.size() > 2) {
            userParams = Arrays.copyOfRange(paramArray, 2, paramArray.length);
        }
        ExplainMode explainMode = isExplain ? ExplainMode.EXPLAIN_ADHOC : ExplainMode.NONE;
        dispatchAdHocCommon(task, handler, ccxn, explainMode, sql, userParams, userPartitionKey, user);
        return null;
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
                          partition,
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

    final void dispatchUpdateApplicationCatalog(StoredProcedureInvocation task,
            boolean useDdlSchema, Connection ccxn, AuthSystem.AuthUser user, boolean isAdmin)
    {
        ParameterSet params = task.getParams();
        final Object [] paramArray = params.toArray();
        // default catalogBytes to null, when passed along, will tell the
        // catalog change planner that we want to use the current catalog.
        byte[] catalogBytes = null;
        Object catalogObj = paramArray[0];
        if (catalogObj != null) {
            if (catalogObj instanceof String) {
                // treat an empty string as no catalog provided
                String catalogString = (String) catalogObj;
                if (!catalogString.isEmpty()) {
                    catalogBytes = Encoder.hexDecode(catalogString);
                }
            } else if (catalogObj instanceof byte[]) {
                // treat an empty array as no catalog provided
                byte[] catalogArr = (byte[]) catalogObj;
                if (catalogArr.length != 0) {
                    catalogBytes = catalogArr;
                }
            }
        }
        String deploymentString = (String) paramArray[1];
        LocalObjectMessage work = new LocalObjectMessage(
                new CatalogChangeWork(
                    m_siteId,
                    task.clientHandle, ccxn.connectionId(), ccxn.getHostnameAndIPAndPort(),
                    isAdmin, ccxn, catalogBytes, deploymentString,
                    task.getProcName(),
                    VoltDB.instance().getReplicationRole() == ReplicationRole.REPLICA,
                    useDdlSchema,
                    m_adhocCompletionHandler, user,
                    null, -1L, -1L
                    ));

        m_mailbox.send(m_plannerSiteId, work);
    }

    private final ClientResponseImpl dispatchUpdateApplicationCatalog(StoredProcedureInvocation task,
            InvocationClientHandler handler, Connection ccxn, AuthSystem.AuthUser user,
            boolean useDdlSchema)
    {
        dispatchUpdateApplicationCatalog(task, useDdlSchema, ccxn, user, handler.isAdmin());
        return null;
    }

    private final ClientResponseImpl dispatchPromote(Procedure sysProc,
            StoredProcedureInvocation task,
            InvocationClientHandler handler,
            Connection ccxn)
    {
        if (VoltDB.instance().getReplicationRole() == ReplicationRole.NONE)
        {
            return gracefulFailureResponse(
                    "@Promote issued on master cluster. No action taken.",
                    task.clientHandle);
        }

        // This only happens on one node so we don't need to pick a leader.
        createTransaction(
                handler.connectionId(),
                task,
                sysProc.getReadonly(),
                sysProc.getSinglepartition(),
                sysProc.getEverysite(),
                0,//No partition needed for multi-part
                task.getSerializedSize(),
                System.nanoTime());

        return null;
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

    private final ClientResponseImpl useSnapshotCatalogToRestoreSnapshotSchema(
            final StoredProcedureInvocation task,
            final InvocationClientHandler handler, final Connection ccxn,
            final AuthUser user
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
            final String path = jsObj.getString(SnapshotUtil.JSON_PATH);
            final String nonce = jsObj.getString(SnapshotUtil.JSON_NONCE);
            final File catalogFH = new VoltFile(path, nonce + ".jar");

            final byte[] catalog;
            try {
                catalog = MiscUtils.fileToBytes(catalogFH);
            } catch (IOException e) {
                log.warn("Unable to access catalog file " + catalogFH, e);
                return unexpectedFailureResponse(
                        "Unable to access catalog file " + catalogFH,
                        task.clientHandle);
            }
            final String dep = new String(catalogContext.getDeploymentBytes(), StandardCharsets.UTF_8);

            final StoredProcedureInvocation catalogUpdateTask = new StoredProcedureInvocation();

            catalogUpdateTask.setProcName("@UpdateApplicationCatalog");
            catalogUpdateTask.setParams(catalog,dep);

            final long alternateConnectionId = VoltProtocolHandler.getNextConnectionId();
            final SimpleClientResponseAdapter alternateAdapter = new SimpleClientResponseAdapter(
                    alternateConnectionId, "Empty database snapshot restore catalog update"
                    );
            final InvocationClientHandler alternateHandler = new InvocationClientHandler() {
                @Override
                public boolean isAdmin() {
                    return handler.isAdmin();
                }
                @Override
                public long connectionId() {
                    return alternateConnectionId;
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
                    dispatch(task, alternateHandler, alternateAdapter, user);
                }
            },
            CoreUtils.SAMETHREADEXECUTOR);
            catalogUpdateTask.setClientHandle(alternateAdapter.registerCallback(catalogUpdateCallback));

            VoltDB.instance().getClientInterface().bindAdapter(alternateAdapter, null);

            dispatchUpdateApplicationCatalog(catalogUpdateTask, alternateHandler, alternateAdapter, user, false);

        } catch (JSONException e) {
            return unexpectedFailureResponse("Unable to parse parameters.", task.clientHandle);
        }
        return null;
    }


    /*
     * Allow the async compiler thread to immediately process completed planning tasks
     * without waiting for the periodic work thread to poll the mailbox.
     */
    private final  AsyncCompilerWorkCompletionHandler m_adhocCompletionHandler = new AsyncCompilerWorkCompletionHandler() {
        @Override
        public void onCompletion(AsyncCompilerResult result) {
            processFinishedCompilerWork(result);
        }
    };

    private final void dispatchAdHocCommon(StoredProcedureInvocation task,
            InvocationClientHandler handler, Connection ccxn, ExplainMode explainMode,
            String sql, Object[] userParams, Object[] userPartitionKey, AuthSystem.AuthUser user) {
        List<String> sqlStatements = SQLLexer.splitStatements(sql);
        String[] stmtsArray = sqlStatements.toArray(new String[sqlStatements.size()]);

        AdHocPlannerWork ahpw = new AdHocPlannerWork(
                m_siteId,
                task.clientHandle, handler.connectionId(),
                handler.isAdmin(), ccxn,
                sql, stmtsArray, userParams, null, explainMode,
                userPartitionKey == null, userPartitionKey,
                task.getProcName(),
                task.getBatchTimeout(),
                VoltDB.instance().getReplicationRole() == ReplicationRole.REPLICA,
                VoltDB.instance().getCatalogContext().cluster.getUseddlschema(),
                m_adhocCompletionHandler, user);
        LocalObjectMessage work = new LocalObjectMessage( ahpw );

        m_mailbox.send(m_plannerSiteId, work);
    }

    /*
     * Invoked from the AsyncCompilerWorkCompletionHandler from the AsyncCompilerAgent thread.
     * Has the effect of immediately handing the completed work to the network thread of the
     * client instance that created the work and then dispatching it.
     */
    public ListenableFutureTask<?> processFinishedCompilerWork(final AsyncCompilerResult result) {
        /*
         * Do the task in the network thread associated with the connection
         * so that access to the CIHM can be lock free for fast path work.
         * Can't access the CIHM from this thread without adding locking.
         */
        final Connection c = (Connection)result.clientData;
        final ListenableFutureTask<?> ft = ListenableFutureTask.create(new Runnable() {
            @Override
            public void run() {
                if (result.errorMsg == null) {
                    if (result instanceof AdHocPlannedStmtBatch) {
                        final AdHocPlannedStmtBatch plannedStmtBatch = (AdHocPlannedStmtBatch) result;
                        ExplainMode explainMode = plannedStmtBatch.getExplainMode();

                        // assume all stmts have the same catalog version
                        if ((plannedStmtBatch.getPlannedStatementCount() > 0) &&
                                (!plannedStmtBatch.getPlannedStatement(0).core.wasPlannedAgainstHash(m_catalogContext.get().getCatalogHash())))
                        {

                            /* The adhoc planner learns of catalog updates after the EE and the
                               rest of the system. If the adhoc sql was planned against an
                               obsolete catalog, re-plan. */
                            LocalObjectMessage work = new LocalObjectMessage(
                                    AdHocPlannerWork.rework(plannedStmtBatch.work, m_adhocCompletionHandler));

                            m_mailbox.send(m_plannerSiteId, work);
                        }
                        else if (explainMode == ExplainMode.EXPLAIN_ADHOC) {
                            processExplainPlannedStmtBatch(plannedStmtBatch);
                        }
                        else if (explainMode == ExplainMode.EXPLAIN_DEFAULT_PROC) {
                            processExplainDefaultProc(plannedStmtBatch);
                        }
                        else {
                            try {
                                createAdHocTransaction(plannedStmtBatch, c);
                            }
                            catch (VoltTypeException vte) {
                                String msg = "Unable to execute adhoc sql statement(s): " + vte.getMessage();
                                writeResponseToConnection(gracefulFailureResponse(msg, result.clientHandle));
                            }
                        }
                    }
                    else if (result instanceof CatalogChangeResult) {
                        final CatalogChangeResult changeResult = (CatalogChangeResult) result;

                        if (changeResult.encodedDiffCommands.trim().length() == 0) {
                            ClientResponseImpl shortcutResponse =
                                    new ClientResponseImpl(
                                            ClientResponseImpl.SUCCESS,
                                            new VoltTable[0], "Catalog update with no changes was skipped.",
                                            result.clientHandle);
                            writeResponseToConnection(shortcutResponse);
                        }
                        else {
                            // create the execution site task
                            StoredProcedureInvocation task = getUpdateCatalogExecutionTask(changeResult);

                            ClientResponseImpl error = null;
                            if ((error = m_permissionValidator.shouldAccept(task.getProcName(), result.user, task,
                                    SystemProcedureCatalog.listing.get(task.getProcName()).asCatalogProcedure())) != null) {
                                writeResponseToConnection(error);
                            }
                            else {
                                /*
                                 * Round trip the invocation to initialize it for command logging
                                 */
                                try {
                                    task = MiscUtils.roundTripForCL(task);
                                } catch (Exception e) {
                                    hostLog.fatal(e);
                                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                                }
                                // initiate the transaction. These hard-coded values from catalog
                                // procedure are horrible, horrible, horrible.
                                createTransaction(changeResult.connectionId,
                                        task, false, false, false, 0, task.getSerializedSize(),
                                        System.nanoTime());
                            }
                        }
                    }
                    else {
                        throw new RuntimeException(
                                "Should not be able to get here (ClientInterface.checkForFinishedCompilerWork())");
                    }
                }
                else {
                    ClientResponseImpl errorResponse =
                        new ClientResponseImpl(
                                (result.errorCode == AsyncCompilerResult.UNINITIALIZED_ERROR_CODE) ? ClientResponse.GRACEFUL_FAILURE : result.errorCode,
                                new VoltTable[0], result.errorMsg,
                                result.clientHandle);
                    writeResponseToConnection(errorResponse);
                }
            }

            private final void writeResponseToConnection(ClientResponseImpl response) {
                ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                response.flattenToBuffer(buf);
                buf.flip();
                c.writeStream().enqueue(buf);
            }
        }, null);
        if (c != null) {
            c.queueTask(ft);
        }

        /*
         * Add error handling in case of an unexpected exception
         */
        ft.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                     ft.get();
                } catch (Exception e) {
                    String realReason = result.errorMsg;
                    // Prefer adding detail to reporting an anonymous exception.
                    // This helped debugging when it caught a programming error
                    // -- not sure if this ever should catch anything in production code
                    // that could be explained in friendlier user terms.
                    // In that case, the root cause stack trace might be more of a distraction.
                    if (realReason == null) {
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        Throwable cause = e.getCause();
                        if (cause != null) {
                            cause.printStackTrace(pw);
                        }
                        pw.flush();
                        realReason = sw.toString();
                    }
                    ClientResponseImpl errorResponse =
                            new ClientResponseImpl(
                                    ClientResponseImpl.UNEXPECTED_FAILURE,
                                    new VoltTable[0], realReason,
                                    result.clientHandle);
                    ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                    buf.putInt(buf.capacity() - 4);
                    errorResponse.flattenToBuffer(buf);
                    buf.flip();
                    c.writeStream().enqueue(buf);
                }
            }
        }, CoreUtils.SAMETHREADEXECUTOR);

        //Return the future task for test code
        return ft;
    }

    /**
     * Take the response from the async ad hoc planning process and put the explain
     * plan in a table with the right format.
     */
    private final void processExplainPlannedStmtBatch(  AdHocPlannedStmtBatch planBatch ) {
        final Connection c = (Connection)planBatch.clientData;
        Database db = m_catalogContext.get().database;
        int size = planBatch.getPlannedStatementCount();

        VoltTable[] vt = new VoltTable[ size ];
        for (int i = 0; i < size; ++i) {
            vt[i] = new VoltTable(new VoltTable.ColumnInfo("EXECUTION_PLAN", VoltType.STRING));
            String str = planBatch.explainStatement(i, db);
            vt[i].addRow(str);
        }

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        vt,
                        null);
        response.setClientHandle( planBatch.clientHandle );
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        c.writeStream().enqueue(buf);
    }

    public static final StoredProcedureInvocation getUpdateCatalogExecutionTask(CatalogChangeResult changeResult) {
        // create the execution site task
           StoredProcedureInvocation task = new StoredProcedureInvocation();
           task.setProcName("@UpdateApplicationCatalog");
           task.setParams(changeResult.encodedDiffCommands,
                          changeResult.catalogHash,
                          changeResult.catalogBytes,
                          changeResult.expectedCatalogVersion,
                          changeResult.deploymentString,
                          changeResult.tablesThatMustBeEmpty,
                          changeResult.reasonsForEmptyTables,
                          changeResult.requiresSnapshotIsolation ? 1 : 0,
                          changeResult.worksWithElastic ? 1 : 0,
                          changeResult.deploymentHash);
           task.clientHandle = changeResult.clientHandle;
           // DR stuff
           task.type = changeResult.invocationType;
           return task;
       }


    /**
     * Explain Proc for a default proc is routed through the regular Explain
     * path using ad hoc planning and all. Take the result from that async
     * process and format it like other explains for procedures.
     */
    private final void processExplainDefaultProc(AdHocPlannedStmtBatch planBatch) {
        final Connection c = (Connection)planBatch.clientData;
        Database db = m_catalogContext.get().database;

        // there better be one statement if this is really sql
        // from a default procedure
        assert(planBatch.getPlannedStatementCount() == 1);
        AdHocPlannedStatement ahps = planBatch.getPlannedStatement(0);
        String sql = new String(ahps.sql, StandardCharsets.UTF_8);
        String explain = planBatch.explainStatement(0, db);

        VoltTable vt = new VoltTable(new VoltTable.ColumnInfo( "SQL_STATEMENT", VoltType.STRING),
                new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));
        vt.addRow(sql, explain);

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        new VoltTable[] { vt },
                        null);
        response.setClientHandle( planBatch.clientHandle );
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        c.writeStream().enqueue(buf);
    }

    private final void createAdHocTransaction(final AdHocPlannedStmtBatch plannedStmtBatch, Connection c)
            throws VoltTypeException
    {
        ByteBuffer buf = null;
        try {
            buf = plannedStmtBatch.flattenPlanArrayToBuffer();
        }
        catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        assert(buf.hasArray());

        // create the execution site task
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        task.setBatchTimeout(plannedStmtBatch.work.m_batchTimeout);
        // pick the sysproc based on the presence of partition info
        // HSQL (or PostgreSQL) does not specifically implement AdHoc SP
        // -- instead, use its always-SP implementation of AdHoc
        boolean isSinglePartition = plannedStmtBatch.isSinglePartitionCompatible() || m_isConfiguredForNonVoltDBBackend;
        int partition = -1;

        if (isSinglePartition) {
            if (plannedStmtBatch.isReadOnly()) {
                task.setProcName("@AdHoc_RO_SP");
            }
            else {
                task.setProcName("@AdHoc_RW_SP");
            }
            int type = VoltType.NULL.getValue();
            // replicated table read is single-part without a partitioning param
            // I copied this from below, but I'm not convinced that the above statement is correct
            // or that the null behavior here either (a) ever actually happens or (b) has the
            // desired intent.
            Object partitionParam = plannedStmtBatch.partitionParam();
            byte[] param = null;
            if (partitionParam != null) {
                type = VoltType.typeFromClass(partitionParam.getClass()).getValue();
                param = VoltType.valueToBytes(partitionParam);
            }
            partition = TheHashinator.getPartitionForParameter(type, partitionParam);

            // Send the partitioning parameter and its type along so that the site can check if
            // it's mis-partitioned. Type is needed to re-hashinate for command log re-init.
            task.setParams(param, (byte)type, buf.array());
        }
        else {
            if (plannedStmtBatch.isReadOnly()) {
                task.setProcName("@AdHoc_RO_MP");
            }
            else {
                task.setProcName("@AdHoc_RW_MP");
            }
            task.setParams(buf.array());
        }
        task.clientHandle = plannedStmtBatch.clientHandle;

        ClientResponseImpl error = null;
        if (VoltDB.instance().getMode() == OperationMode.PAUSED &&
                !plannedStmtBatch.isReadOnly() && !plannedStmtBatch.adminConnection) {
            error = new ClientResponseImpl(
                    ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0],
                    "Server is paused and is available in read-only mode - please try again later",
                    plannedStmtBatch.clientHandle);
            ByteBuffer buffer = ByteBuffer.allocate(error.getSerializedSize() + 4);
            buffer.putInt(buffer.capacity() - 4);
            error.flattenToBuffer(buffer).flip();
            c.writeStream().enqueue(buffer);
        }
        else
        if ((error = m_permissionValidator.shouldAccept(task.getProcName(), plannedStmtBatch.work.user, task,
                SystemProcedureCatalog.listing.get(task.getProcName()).asCatalogProcedure())) != null) {
            ByteBuffer buffer = ByteBuffer.allocate(error.getSerializedSize() + 4);
            buffer.putInt(buffer.capacity() - 4);
            error.flattenToBuffer(buffer).flip();
            c.writeStream().enqueue(buffer);
        }
        else
        if ((error = m_invocationValidator.shouldAccept(task.getProcName(), plannedStmtBatch.work.user, task,
                SystemProcedureCatalog.listing.get(task.getProcName()).asCatalogProcedure())) != null) {
            ByteBuffer buffer = ByteBuffer.allocate(error.getSerializedSize() + 4);
            buffer.putInt(buffer.capacity() - 4);
            error.flattenToBuffer(buffer).flip();
            c.writeStream().enqueue(buffer);
        }
        else {
            /*
             * Round trip the invocation to initialize it for command logging
             */
            try {
                task = MiscUtils.roundTripForCL(task);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // initiate the transaction
            createTransaction(plannedStmtBatch.connectionId, task,
                    plannedStmtBatch.isReadOnly(), isSinglePartition, false,
                    partition,
                    task.getSerializedSize(), System.nanoTime());
        }
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public boolean createTransaction(
            final long connectionId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partition,
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
                partition,
                messageSize,
                nowNanos,
                false);  // is for replay.
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    @SuppressWarnings("unused")
    public  boolean createTransaction(
            final long connectionId,
            final long txnId,
            final long uniqueId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partition,
            final int messageSize,
            long nowNanos,
            final boolean isForReplay)
    {
        assert(!isSinglePartition || (partition >= 0));
        final ClientInterfaceHandleManager cihm = m_cihm.get(connectionId);
        if (cihm == null) {
            hostLog.warn("InvocationDispatcher.createTransaction request rejected. "
                    + "This is likely due to VoltDB ceasing client communication as it "
                    + "shuts down.");
            return false;
        }

        Long initiatorHSId = null;
        boolean isShortCircuitRead = false;

        /*
         * ReadLevel.FAST:
         * If this is a read only single part, check if there is a local replica,
         * if there is, send it to the replica as a short circuit read
         *
         * ReadLevel.SAFE:
         * Send the read to the partition leader always (reads & writes)
         *
         * Someday could support per-transaction consistency for reads.
         */
        if (isSinglePartition && !isEveryPartition) {
            if (isReadOnly && (m_defaultConsistencyReadLevel == ReadLevel.FAST)) {
                initiatorHSId = m_localReplicas.get().get(partition);
            }
            if (initiatorHSId != null) {
                isShortCircuitRead = true;
            } else {
                initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(partition);
            }
        }
        else {
            //Multi-part transactions go to the multi-part coordinator
            initiatorHSId = m_cartographer.getHSIdForMultiPartitionInitiator();
            // Treat all MP reads as short-circuit since they can run out-of-order
            // from their arrival order due to the MP Read-only execution pool
            if (isReadOnly) {
                isShortCircuitRead = true;
            }
        }

        if (initiatorHSId == null) {
            hostLog.error("Failed to find master initiator for partition: "
                    + Integer.toString(partition) + ". Transaction not initiated.");
            return false;
        }

        long handle = cihm.getHandle(isSinglePartition, partition, invocation.getClientHandle(),
                messageSize, nowNanos, invocation.getProcName(), initiatorHSId, isReadOnly, isShortCircuitRead);

        Iv2InitiateTaskMessage workRequest =
            new Iv2InitiateTaskMessage(m_siteId,
                    initiatorHSId,
                    Iv2InitiateTaskMessage.UNUSED_TRUNC_HANDLE,
                    txnId,
                    uniqueId,
                    isReadOnly,
                    isSinglePartition,
                    invocation,
                    handle,
                    connectionId,
                    isForReplay);

        Iv2Trace.logCreateTransaction(workRequest);
        m_mailbox.send(initiatorHSId, workRequest);
        return true;
    }

    final static int getPartitionForProcedure(Procedure procedure, StoredProcedureInvocation task) {
        final CatalogContext.ProcedurePartitionInfo ppi =
                (CatalogContext.ProcedurePartitionInfo)procedure.getAttachment();
        if (procedure.getSinglepartition()) {
            // break out the Hashinator and calculate the appropriate partition
            return getPartitionForProcedure( ppi.index, ppi.type, task);
        } else {
            return -1;
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


    /**
     * Identify the partition for an execution site task.
     * @return The partition best set up to execute the procedure.
     */
    final static int getPartitionForProcedure(int partitionIndex, VoltType partitionType, StoredProcedureInvocation task) {
        Object invocationParameter = task.getParameterAtIndex(partitionIndex);
        return TheHashinator.getPartitionForParameter(partitionType, invocationParameter);
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
}
