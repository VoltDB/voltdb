/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZooKeeperLock;
import org.voltdb.iv2.LeaderCache;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;
import org.voltdb.iv2.MigratePartitionLeaderInfo;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.Sets;

/**
 * VoltZK provides constants for all voltdb-registered
 * ZooKeeper paths.
 */
public class VoltZK {

    static final VoltLogger tmLog = new VoltLogger("TM");
    private final static String ERROR_DECOMMISSION = "while decommissioning replicas is in progress";
    private final static String ERROR_REDUCEDCLUSTERSAFETY = "while cluster is in reduced safety mode";
    private final static String ERROR_REJOIN = "while node rejoin is in progress";
    private final static String ERROR_LEADER_MIGRATION = "while leader migration is in progress";
    private final static String ERROR_CATALOG_UPDATE = "while catalog update is in progress";
    private final static String ERROR_ELASTIC_OPERATION = "while elastic operation is in progress";
    private final static String ERROR_MP_REPAIR = "while leader promotion or transaction repair are in progress";
    private final static String ERROR_LICENSE_UPDATE = "while live license update is in progress";

    public static final String root = "/db";

    public static final String buildstring = "/db/buildstring";
    public static final String catalogbytes = "/db/catalogbytes";
    public static final String license = "/db/license";
    //This node doesn't mean as much as it used to, it is accurate at startup
    //but isn't updated after elastic operation. We use the cartographer for most things
    //now
    public static final String topology = "/db/topology";
    public static final String replicationconfig = "/db/replicationconfig";
    public static final String rejoinLock = "/db/rejoin_lock";
    public static final String perPartitionTxnIds = "/db/perPartitionTxnIds";
    public static final String operationMode = "/db/operation_mode";
    public static final String exportGenerations = "/db/export_generations";
    public static final String exportCoordination = "/db/export_coordination";

    // configuration (ports, interfaces, ...)
    public static final String cluster_metadata = "/db/cluster_metadata";

    // localMetadata json property names
    public static final String drPublicHostProp = "drPublicHost";
    public static final String drPublicPortProp = "drPublicPort";

    /*
     * mailboxes
     *
     * Contents in the mailbox ZK nodes are all simple JSON objects. No nested
     * objects should be stored in them. They must all have a field called
     * "HSId" that maps to their host-site ID. Some of them may also contain a
     * "partitionId" field.
     */
    public static enum MailboxType {
        ClientInterface, ExecutionSite, Initiator, StatsAgent,
        OTHER
    }

    public static final String nt_mailboxes = "/db/cl_mailboxes";

    // snapshot and command log
    public static final String completed_snapshots = "/db/completed_snapshots";
    public static final String nodes_currently_snapshotting = "/db/nodes_currently_snapshotting";
    public static final String restore = "/db/restore";
    public static final String restore_barrier = "/db/restore_barrier";
    public static final String restore_barrier2 = "/db/restore_barrier2";
    public static final String restore_snapshot_id = "/db/restore/snapshot_id";
    public static final String request_truncation_snapshot = "/db/request_truncation_snapshot";
    public static final String snapshot_truncation_master = "/db/snapshot_truncation_master";
    public static final String test_scan_path = "/db/test_scan_path";   // (test only)
    public static final String user_snapshot_request = "/db/user_snapshot_request";
    public static final String user_snapshot_response = "/db/user_snapshot_response";
    public static final String commandlog_init_barrier = "/db/commmandlog_init_barrier";

    // leader election

    // root for MigratePartitionLeader information nodes
    public static final String migrate_partition_leader_info = "/core/migrate_partition_leader_info";
    public static final String drConsumerPartitionMigration = "/db/dr_consumer_partition_migration";

    public static final String iv2masters = "/db/iv2masters";
    public static final String iv2appointees = "/db/iv2appointees";
    public static final String iv2mpi = "/db/iv2mpi";
    public static final String leaders = "/db/leaders";
    public static final String leaders_initiators = "/db/leaders/initiators";
    public static final String leaders_globalservice = "/db/leaders/globalservice";
    public static final String lastKnownLiveNodes = "/db/lastKnownLiveNodes";

    public static final String debugLeadersInfo(ZooKeeper zk) {
        StringBuilder builder = new StringBuilder("ZooKeeper:\n");
        printZKDir(zk, iv2masters, builder);
        printZKDir(zk, iv2appointees, builder);
        printZKDir(zk, iv2mpi, builder);
        printZKDir(zk, leaders_initiators, builder);
        printZKDir(zk, leaders_globalservice, builder);

        return builder.toString();
    }

    public static final void printZKDir(ZooKeeper zk, String dir, StringBuilder builder) {
        builder.append(dir).append(":\t ");
        try {
            List<String> keys = zk.getChildren(dir, null);
            boolean isData = false;
            for (String key: keys) {
                String path = ZKUtil.joinZKPath(dir, key);
                byte[] arr = zk.getData(path, null, null);

                if (arr != null) {
                    String data = new String(arr, "UTF-8");
                    if (iv2masters.equals(dir) || iv2appointees.equals(dir)) {
                        LeaderCallBackInfo info = LeaderCache.buildLeaderCallbackFromString(data);
                        data = info.toString();
                    }
                    isData = true;
                    builder.append(key).append(" -> ").append(data).append(",");
                } else {
                    // path may be a dir instead
                    List<String> children = zk.getChildren(path, null);
                    if (children != null) {
                        builder.append("\n");
                        printZKDir(zk, path, builder);
                    }
                }
            }
            if (isData) {
                builder.append("\n");
            }
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            builder.append(e.getMessage());
        }
    }

    // flag of initialization process complete
    public static final String init_completed = "/db/init_completed";

    // start action of node in the current system (ephemeral)
    public static final String start_action = "/db/start_action";
    public static final String start_action_node = ZKUtil.joinZKPath(start_action, "node_");

    /*
     * Processes that want to be mutually exclusive create children here
     */
    public static final String actionBlockers = "/db/action_blockers";
    // being able to use as constant string

    public static final String migrate_partition_leader = "migrate_partition_leader_blocker";
    public static final String migratePartitionLeaderBlocker = actionBlockers + "/" + migrate_partition_leader;

    // three elastic blockers
    // elasticOperationInProgress blocks the rejoin (create in init state, release before data migration start)
    // banElasticOperation blocks elastic operation (currently created by DRProducer if the agreed protocol version
    //                                     for the mesh does not support elastic operation during DR (i.e. version <= 7).
    //                                     It is now only released after a DR global reset.)
    // elasticMigration only blocks SPI Migration
    private static final String leafNodeElasticOperationInProgress = "elastic_blocker";
    public static final String elasticOperationInProgress = actionBlockers + "/" + leafNodeElasticOperationInProgress;
    private static final String leafNodeBanElasticOperation = "no_elastic_blocker";
    public static final String banElasticOperation = actionBlockers + "/" + leafNodeBanElasticOperation;
    private static final String leafNodeElasticMigration = "elastic_migration_blocker";
    public static final String elasticMigration = actionBlockers + "/" + leafNodeElasticMigration;

    private static final String leafNodeRejoinInProgress = "rejoin_blocker";
    public static final String rejoinInProgress = actionBlockers + "/" + leafNodeRejoinInProgress;
    private static final String leafNodeCatalogUpdateInProgress = "uac_nt_blocker";
    public static final String catalogUpdateInProgress = actionBlockers + "/" + leafNodeCatalogUpdateInProgress;

    private static final String leafNodeLicenseUpdateInProgress = "license_update_blocker";
    public static final String licenseUpdateInProgress = actionBlockers + "/" + leafNodeLicenseUpdateInProgress;

    //register partition while the partition elects a new leader upon node failure
    private static final String mpRepairBlocker = "mp_repair_blocker";
    public static final String mpRepairInProgress = actionBlockers + "/" + mpRepairBlocker;

    private static final String decommissionReplicas = "decommissionReplicas_blocker";
    public static final String decommissionReplicasInProgress = actionBlockers + "/" + decommissionReplicas;

    // ReducedClusterSafety has longer scope than decommissionReplicasBlocker (will not been release until transfer back to full cluster mode)
    // It prohibit cluster topology changed related operation
    // but do allow snapshot operation
    private static final String leafReducedClusterSafety = "reduced_clustersafety_block";
    public static final String reducedClusterSafety = actionBlockers + "/" + leafReducedClusterSafety;

    private static final String snapshotBlocker = "snapshot_blocker";
    public static final String snapshotSetupInProgress = actionBlockers + "/" + snapshotBlocker;

    public static final String request_truncation_snapshot_node = ZKUtil.joinZKPath(request_truncation_snapshot, "request_");

    // Synchronized State Machine
    public static final String syncStateMachine = "/db/synchronized_states";

    // Settings base
    public static final String settings_base = "/db/settings";

    // Cluster settings
    public static final String cluster_settings = ZKUtil.joinZKPath(settings_base, "cluster");

    // Shutdown save snapshot guard
    public static final String shutdown_save_guard = "/db/shutdown_save_guard";

    // Host ids that be stopped by calling @StopNode
    public static final String host_ids_be_stopped = "/db/host_ids_be_stopped";

    public static final String actionLock = "/db/action_lock";

    public static final String hashMismatchedReplicas = "/db/mismatched";

    public static final String trigger_txn_restart = "/db/restart";

    // Persistent nodes (mostly directories) to create on startup
    public static final String[] ZK_HIERARCHY = {
            root,
            cluster_metadata,
            drConsumerPartitionMigration,
            operationMode,
            iv2masters,
            iv2appointees,
            iv2mpi,
            leaders,
            leaders_initiators,
            leaders_globalservice,
            lastKnownLiveNodes,
            syncStateMachine,
            settings_base,
            cluster_settings,
            actionBlockers,
            request_truncation_snapshot,
            host_ids_be_stopped,
            actionLock,
            hashMismatchedReplicas,
            catalogbytes,
            nt_mailboxes
    };

    /**
     * Race to create the persistent nodes.
     */
    public static void createPersistentZKNodes(ZooKeeper zk) {
        LinkedList<ZKUtil.StringCallback> callbacks = new LinkedList<ZKUtil.StringCallback>();
        for (int i=0; i < VoltZK.ZK_HIERARCHY.length; i++) {
            ZKUtil.StringCallback cb = new ZKUtil.StringCallback();
            callbacks.add(cb);
            zk.create(VoltZK.ZK_HIERARCHY[i], null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, null);
        }
        for (ZKUtil.StringCallback cb : callbacks) {
            try {
                cb.get();
            } catch (org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException e) {
                // this is an expected race.
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }
        }
    }

    /**
     * Helper method for parsing mailbox node contents into Java objects.
     * @throws JSONException
     */
    public static List<MailboxNodeContent> parseMailboxContents(List<String> jsons) throws JSONException {
        ArrayList<MailboxNodeContent> objects = new ArrayList<MailboxNodeContent>(jsons.size());
        for (String json : jsons) {
            MailboxNodeContent content = null;
            JSONObject jsObj = new JSONObject(json);
            long HSId = jsObj.getLong("HSId");
            Integer partitionId = null;
            if (jsObj.has("partitionId")) {
                partitionId = jsObj.getInt("partitionId");
            }
            content = new MailboxNodeContent(HSId, partitionId);
            objects.add(content);
        }
        return objects;
    }

    public static void updateClusterMetadata(Map<Integer, String> clusterMetadata) throws Exception {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();

        List<String> metadataNodes = zk.getChildren(VoltZK.cluster_metadata, false);

        Set<Integer> hostIds = new HashSet<Integer>();
        for (String hostId : metadataNodes) {
            hostIds.add(Integer.valueOf(hostId));
        }

        /*
         * Remove anything that is no longer part of the cluster
         */
        Set<Integer> keySetCopy = new HashSet<Integer>(clusterMetadata.keySet());
        keySetCopy.removeAll(hostIds);
        for (Integer failedHostId : keySetCopy) {
            clusterMetadata.remove(failedHostId);
        }

        /*
         * Add anything that is new
         */
        Set<Integer> hostIdsCopy = new HashSet<Integer>(hostIds);
        hostIdsCopy.removeAll(clusterMetadata.keySet());
        List<Pair<Integer, ZKUtil.ByteArrayCallback>> callbacks =
            new ArrayList<Pair<Integer, ZKUtil.ByteArrayCallback>>();
        for (Integer hostId : hostIdsCopy) {
            ZKUtil.ByteArrayCallback cb = new ZKUtil.ByteArrayCallback();
            callbacks.add(Pair.of(hostId, cb));
            zk.getData(VoltZK.cluster_metadata + "/" + hostId, false, cb, null);
        }

        for (Pair<Integer, ZKUtil.ByteArrayCallback> p : callbacks) {
            Integer hostId = p.getFirst();
            ZKUtil.ByteArrayCallback cb = p.getSecond();
            try {
               clusterMetadata.put( hostId, new String(cb.get(), "UTF-8"));
            } catch (KeeperException.NoNodeException e){}
        }
    }

    public static Pair<String, Integer> getDRPublicInterfaceAndPortFromMetadata(String metadata)
    throws IllegalArgumentException {
        try {
            JSONObject obj = new JSONObject(metadata);
            // Precedence order for host:port on which consumers should connect:
            // - drPublic
            // - drInterface
            // - 0th element in interfaces
            String hostName = obj.getString(drPublicHostProp);
            if (hostName == null || hostName.isEmpty()) {
                hostName = obj.getString("drInterface");
            }
            if (hostName == null || hostName.length() <= 0) {
                hostName = obj.getJSONArray("interfaces").getString(0);
            }
            assert(hostName != null);
            assert(hostName.length() > 0);

            int port = obj.getInt(drPublicPortProp);
            if (port == VoltDB.DISABLED_PORT) {
                port = obj.getInt("drPort");
            }

            return Pair.of(hostName, port);
        } catch (JSONException e) {
            throw new IllegalArgumentException("Error parsing host metadata", e);
        }
    }

    /**
     * Convert a list of ZK nodes named HSID_SUFFIX (such as that used by LeaderElector)
     * into a list of HSIDs.
     */
    public static List<Long> childrenToReplicaHSIds(Collection<String> children)
    {
        List<Long> replicas = new ArrayList<Long>(children.size());
        for (String child : children) {
            long HSId = Long.parseLong(CoreZK.getPrefixFromChildName(child));
            replicas.add(HSId);
        }
        return replicas;
    }

    public static void createStartActionNode(ZooKeeper zk, final int hostId, StartAction action) {
        byte [] startActionBytes = null;
        try {
            startActionBytes = action.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            VoltDB.crashLocalVoltDB("Utf-8 encoding is not supported in current platform", false, e);
        }

        zk.create(VoltZK.start_action_node + hostId, startActionBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                new ZKUtil.StringCallback(), null);
    }

    public static int getHostIDFromChildName(String childName) {
        return Integer.parseInt(childName.split("_")[1]);
    }

    /**
     * @param zk
     * @param node
     * @return true when @param zk @param node exists, false otherwise
     */
    public static boolean zkNodeExists(ZooKeeper zk, String node)
    {
        try {
            if (zk.exists(node, false) == null) {
                return false;
            }
        } catch (KeeperException | InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to check ZK node exists: " + node, true, e);
        }
        return true;
    }

    /**
     * Create a ZK node under action blocker directory. Exclusive execution of elastic operation, rejoin or catalog
     * update is checked.
     * </p>
     * Catalog update can not happen during node rejoin.
     * </p>
     * Node rejoin can not happen during catalog update or elastic operation.
     * </p>
     * Elastic operation can not happen during node rejoin or catalog update.
     *
     * @param zk
     * @param node
     * @param hostLog
     * @param request
     * @return null for success, non-null for error string
     */
    public static String createActionBlocker(ZooKeeper zk, String node, CreateMode mode, VoltLogger hostLog, String request) {
        //Acquire a lock before creating a blocker and validate actions.
        ZooKeeperLock zklock = new ZooKeeperLock(zk, VoltZK.actionLock, "lock");
        String lockingMessage = null;
        try {
            if(!zklock.acquireLockWithTimeout(TimeUnit.SECONDS.toMillis(60))) {
                lockingMessage = "Could not acquire a lock to create action blocker:" + request;
            } else {
                lockingMessage = setActionBlocker(zk, node, mode, hostLog, request);
            }
        } finally {
            try {
                zklock.releaseLock();
            } catch (IOException e) {}
        }
        return lockingMessage;
    }

    private static String setActionBlocker(ZooKeeper zk, String node, CreateMode mode, VoltLogger hostLog, String request) {

        try {
            zk.create(node,
                      null,
                      Ids.OPEN_ACL_UNSAFE,
                      mode);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                VoltDB.crashLocalVoltDB("Unable to create action blocker " + node, true, e);
            }
            // node exists
            return "Invalid " + request + " request: Can't do " + request +
                    " while another one is in progress. Please retry " + request + " later.";
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to create action blocker " + node, true, e);
        }

        // Validate exclusive access of elastic operation, rejoin, MigratePartitionLeader and catalog update.
        String errorMsg = null;
        try {
            List<String> blockers = zk.getChildren(VoltZK.actionBlockers, false);
            switch (node) {
            case catalogUpdateInProgress:
                if (blockers.contains(leafNodeRejoinInProgress)) {
                    errorMsg = ERROR_REJOIN;
                } else if (blockers.contains(mpRepairBlocker)){
                    // Avoid UAC during MP repair or promotion since UAC will invoke GlobalServiceElector to
                    // register other promotable services while MPI is accepting promotion
                    errorMsg = ERROR_MP_REPAIR;
                } else if (blockers.contains(leafNodeElasticOperationInProgress)) {
                    errorMsg = ERROR_ELASTIC_OPERATION;
                }
                break;
            case rejoinInProgress:
                // node rejoin can not happen during UAC or elastic operation
                if (blockers.contains(leafNodeCatalogUpdateInProgress)) {
                    errorMsg = ERROR_CATALOG_UPDATE;
                } else if (blockers.contains(leafNodeElasticOperationInProgress)) {
                    errorMsg = ERROR_ELASTIC_OPERATION;
                } else if (blockers.contains(migrate_partition_leader)){
                    errorMsg = ERROR_LEADER_MIGRATION;
                } else if (blockers.contains(mpRepairBlocker)){
                    // Upon node failures, a MP repair blocker may be registered right before they
                    // unregistered after repair is done. Let rejoining nodes wait to avoid any
                    // interference with the transaction repair process.
                    errorMsg = ERROR_MP_REPAIR;
                } else if (blockers.contains(decommissionReplicas)){
                    errorMsg = ERROR_DECOMMISSION;
                } else if (blockers.contains(leafReducedClusterSafety)){
                    errorMsg = ERROR_REDUCEDCLUSTERSAFETY;
                } else if (blockers.contains(leafNodeLicenseUpdateInProgress)) {
                    errorMsg = ERROR_LICENSE_UPDATE;
                }
                break;
            case elasticOperationInProgress:
                // elastic operation can not happen during node rejoin
                if (blockers.contains(leafNodeRejoinInProgress)) {
                    errorMsg = ERROR_REJOIN;
                } else if (blockers.contains(leafNodeCatalogUpdateInProgress)) {
                    errorMsg = ERROR_CATALOG_UPDATE;
                } else if (blockers.contains(leafNodeBanElasticOperation)) {
                    errorMsg = "while elastic operation is blocked by DR established with a cluster that "
                            + "does not support remote elastic operation during DR. "
                            + "DR needs to be reset before elastic operation is allowed again.";
                } else if ( blockers.contains(migrate_partition_leader)) {
                    errorMsg = ERROR_LEADER_MIGRATION;
                } else if (blockers.contains(decommissionReplicas)){
                    errorMsg = ERROR_DECOMMISSION;
                } else if (blockers.contains(leafReducedClusterSafety)){
                    errorMsg = ERROR_REDUCEDCLUSTERSAFETY;
                }
                break;
            case migratePartitionLeaderBlocker:
                //MigratePartitionLeader can not happen when join (before data fully migrated), rejoin, catalog update, or repair is in progress.
                blockers.remove(leafNodeBanElasticOperation);
                if (blockers.size() > 1) {
                    errorMsg = ERROR_ELASTIC_OPERATION;
                } else if (blockers.contains(decommissionReplicas)){
                    errorMsg = ERROR_DECOMMISSION;
                } else if (blockers.contains(leafReducedClusterSafety)){
                    errorMsg = ERROR_REDUCEDCLUSTERSAFETY;
                }
                break;
            case elasticMigration:
                // elastic operation balancePartition currently cannot coexist with partition leader migration
               if (blockers.contains(migrate_partition_leader)) {
                   errorMsg = ERROR_LEADER_MIGRATION;
               }
               break;
            case banElasticOperation:
                if (blockers.contains(leafNodeElasticOperationInProgress)) {
                    errorMsg = ERROR_ELASTIC_OPERATION;
                }
                break;
            case mpRepairInProgress:
                break;
            case decommissionReplicasInProgress:
                if (blockers.contains(snapshotBlocker)) {
                    errorMsg = "while snapshot is in progress";
                }
                break;
            case reducedClusterSafety:
                break;
            case snapshotSetupInProgress:
                if (blockers.contains(decommissionReplicas)) {
                    errorMsg = ERROR_DECOMMISSION;
                }
                break;
            case licenseUpdateInProgress:
                if (blockers.contains(leafNodeRejoinInProgress)) {
                    errorMsg = ERROR_REJOIN;
                }
                break;
            default:
                // not possible
                VoltDB.crashLocalVoltDB("Invalid request " + node , true, new RuntimeException("Non-supported " + request));
            }
        } catch (Exception e) {
            // should not be here
            VoltDB.crashLocalVoltDB("Error reading children of ZK " + VoltZK.actionBlockers + ": " + e.getMessage(), true, e);
        }

        if (errorMsg != null) {
            VoltZK.removeActionBlocker(zk, node, hostLog);
            return "Can't do " + request + " " + errorMsg;
        }

        hostLog.info("Create action blocker " + node + " successfully.");
        // successfully create a ZK node
        return null;
    }

    public static boolean removeActionBlocker(ZooKeeper zk, String node, VoltLogger log) {
        try {
            zk.delete(node, -1);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                return true;
            }
            if (log != null) {
                log.error("Failed to remove action blocker: " + node + "\n" + e.getMessage(), e);
            }
            return false;
        } catch (InterruptedException e) {
            if (log != null) {
                log.error("Failed to remove action blocker: " + node + "\n" + e.getMessage(), e);
            }
            return false;
        }
        if (log != null) {
            log.info("Remove action blocker " + node + " successfully.");
        }
        return true;
    }

    public static void removeStopNodeIndicator(ZooKeeper zk, String node, VoltLogger log) {
        try {
            ZKUtil.deleteRecursively(zk, node);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                log.debug("Failed to remove stop node indicator " + node + " on ZK: " + e.getMessage());
            }
            return;
        } catch (InterruptedException ignore) {}
    }

    /**
     * Save MigratePartitionLeader information for error handling
     */
    public static boolean createMigratePartitionLeaderInfo(ZooKeeper zk, MigratePartitionLeaderInfo info) {
        try {
            zk.create(migrate_partition_leader_info, info.toBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                try {
                    zk.setData(migrate_partition_leader_info, info.toBytes(), -1);
                } catch (KeeperException | InterruptedException | JSONException e1) {
                }
                return false;
            }

            org.voltdb.VoltDB.crashLocalVoltDB("Unable to create MigratePartitionLeader Indicator", true, e);
        } catch (InterruptedException | JSONException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Unable to create MigratePartitionLeader Indicator", true, e);
        }

        return true;
    }

    /**
     * get MigratePartitionLeader information
     */
    public static MigratePartitionLeaderInfo getMigratePartitionLeaderInfo(ZooKeeper zk) {
        try {
            byte[] data = zk.getData(migrate_partition_leader_info, null, null);
            if (data != null) {
                MigratePartitionLeaderInfo info = new MigratePartitionLeaderInfo(data);
                return info;
            }
        } catch (KeeperException | InterruptedException | JSONException e) {
        }
        return null;
    }

    /**
     * Removes the MigratePartitionLeader info
     */
    public static void removeMigratePartitionLeaderInfo(ZooKeeper zk) {
        try {
            zk.delete(migrate_partition_leader_info, -1);
        } catch (KeeperException | InterruptedException e) {
        }
    }

    /**
     * @param zk ZooKeeper
     * @return true if any hosts work on snapshot
     */
    public static boolean hasHostsSnapshotting(ZooKeeper zk) {
        try {
            List<String> nodesSnapshotting = zk.getChildren(VoltZK.nodes_currently_snapshotting, false);
            return (!nodesSnapshotting.isEmpty());
        } catch (KeeperException | InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to read snapshotting hosts.", true, e);
        }
        return false;
    }

    /**
     * Store hash mismatched replicas
     */
    public static void addHashMismatchedSite(ZooKeeper zk, long hsId) {
        try {
            int hostId = CoreUtils.getHostIdFromHSId(hsId);
            // With 10000x - site id, no sites would have the same idPath in reality.
            String idPath = String.valueOf(hostId * 10000) + CoreUtils.getSiteIdFromHSId(hsId);
            String id = Long.toString(Long.MAX_VALUE) + "/" + Long.toString(hsId);
            zk.create(ZKUtil.joinZKPath(hashMismatchedReplicas, idPath),
                    id.getBytes(Charsets.UTF_8),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to store hash mismatched replica info", true, e);
        }
    }

    public static void removeHashMismatchedSite(ZooKeeper zk, long hsId) {
        try {
            int hostId = CoreUtils.getHostIdFromHSId(hsId);
            String idPath = String.valueOf(hostId * 10000) + CoreUtils.getSiteIdFromHSId(hsId);
            final String path = ZKUtil.joinZKPath(hashMismatchedReplicas, idPath);
            zk.delete(path, -1);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to delete hash mismatched replica info", true, e);
        }
    }

    public static boolean hasHashMismatchedSite(ZooKeeper zk) {
        try {
            List<String> mismatchedReplicas = zk.getChildren(hashMismatchedReplicas, false);
            return (!mismatchedReplicas.isEmpty());
        } catch (KeeperException | InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to read hash mismatched sites.", true, e);
        }
        return false;
    }

    public static void registerMailBoxForNT(ZooKeeper zk, long hsid) {
        String path = ZKUtil.joinZKPath(nt_mailboxes, Long.toString(hsid));
        try {
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to add client interface mailbox", true, e);
        }
    }

    public static Set<Long> getMailBoxesForNT(ZooKeeper zk) {
        Set<Long> mailboxes = Sets.newHashSet();
        try {
            List<String> clMailboxes = zk.getChildren(nt_mailboxes, false);
            mailboxes = clMailboxes.stream().map(Long::valueOf).collect(Collectors.toSet());
        } catch (KeeperException | InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to read client interface mailboxes.", true, e);
        }
        return mailboxes;
    }
}
