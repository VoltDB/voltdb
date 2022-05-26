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

import io.netty.handler.ssl.OpenSsl;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;

class DailyLogging {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static boolean firstTimeRun = true;

    /**
     * This is called at startup, and on a daily basis, to log useful
     * information about the configuration and state of the node and
     * cluster.
     */
    static void logInfo() {
        hostLog.info("==== Start of daily system info log ====");
        try {
            VoltDBInterface inst = VoltDB.instance();
            Configuration config = inst.getConfig();
            String ver = inst.getVersionString();
            String build = inst.getBuildString();
            if (build.contains("_")) build =  build.split("_", 2)[1];
            hostLog.infoFmt("VoltDB %s, build %s", ver, build);
            hostLog.infoFmt("Host id of this node is: %d", inst.getMyHostId());
            long uptime = Duration.between(inst.getHostStartTime(), Instant.now()).toMillis();
            hostLog.infoFmt("Local node state: %s, uptime: %s", inst.getNodeState(), MiscUtils.formatUptime(uptime));
            hostLog.infoFmt("Cluster state: %s, uptime: %s", inst.getMode(), MiscUtils.formatUptime(inst.getClusterUptime()));
            hostLog.infoFmt("Cluster safety: %s", inst.isMasterOnly() ? "reduced" : "full");
            hostLog.infoFmt("URL of deployment info: %s", config.m_pathToDeployment);
            hostLog.infoFmt("Running under Kubernetes: %b", config.runningUnderKubernetes());

            logDebuggingInfo();
            logSystemSettingFromCatalogContext();
            inst.getLicensing().logLicensingInfo();
        }
        catch (Exception ex) {
            hostLog.warn("Exception in daily logging task", ex);
        }
        hostLog.info("==== End of daily system info log ====");
        firstTimeRun = false;
    }

    /*
     * Internal subroutine of above
     *
     * Important: updates the startMode value in the
     * database as a side effect.
     */
    private static void logDebuggingInfo() {
        VoltDBInterface inst = VoltDB.instance();
        Configuration config = inst.getConfig();
        CatalogContext catalog = inst.getCatalogContext();
        RealVoltDB realVolt = (RealVoltDB)inst;

        hostLog.infoFmt("Database start action is %s", config.m_startAction);
        hostLog.infoFmt("Listening for native wire protocol clients on port %d.", config.m_port);
        hostLog.infoFmt("Listening for admin wire protocol clients on port %d.", config.m_adminPort);

        if ((firstTimeRun ? inst.getStartMode() : inst.getMode()) == OperationMode.PAUSED) {
            String verb = firstTimeRun ? "Started" : "Running";
            hostLog.infoFmt("%s in admin mode; clients on port %d will be rejected.", verb, config.m_port);
        }

        if (inst.getReplicationRole() == ReplicationRole.REPLICA) {
            hostLog.infoFmt("Started as %s cluster. Clients can only call read-only procedures.",
                            inst.getReplicationRole().toString().toLowerCase());
        }

        String httpPortExtraLogMessage = realVolt.m_httpPortExtraLogMessage;
        if (httpPortExtraLogMessage != null) {
            hostLog.info(httpPortExtraLogMessage);
        }

        if (config.m_httpPort != -1) {
            hostLog.infoFmt("Local machine HTTP monitoring is listening on port %d.", config.m_httpPort);
        }
        else {
            hostLog.infoFmt("Local machine HTTP monitoring is disabled.");
        }

        boolean jsonEnabled = realVolt.m_jsonEnabled;
        if (jsonEnabled) {
            hostLog.infoFmt("Json API over HTTP enabled at path /api/2.0/, listening on port %d.",
                            config.m_httpPort);
        }
        else {
            hostLog.info("Json API is disabled.");
        }

        if (config.m_sslEnable) {
            hostLog.info("OpenSsl is " + (OpenSsl.isAvailable() ? "enabled" : "disabled"));
        }

        // java heap size
        long javamaxheapmem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        javamaxheapmem /= (1024 * 1024);
        hostLog.infoFmt("Maximum usable Java heap set to %d MB.", javamaxheapmem);

        // Computed minimum heap requirement
        long minRqt = realVolt.computeMinimumHeapRqt();
        hostLog.infoFmt("Minimum required Java heap for catalog and server config is %d MB.", minRqt);

        SortedMap<String, String> dbgMap = catalog.getDebuggingInfoFromCatalog(true);
        for (String line : dbgMap.values()) {
            hostLog.info(line);
        }

        // print out a bunch of useful system info
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        String[] lines = pp.toLogLines(inst.getVersionString()).split("\n");
        for (String line : lines) {
            hostLog.info(line.trim());
        }

        if (catalog.cluster.getDrconsumerenabled() || catalog.cluster.getDrproducerenabled()) {
            String verb = firstTimeRun ? "initializing" : "initialized";
            hostLog.infoFmt("DR %s with Cluster Id %d. The DR cluster was first started at %s.",
                            verb, catalog.cluster.getDrclusterid(), new Date(inst.getClusterCreateTime()).toString());
        }

        HostMessenger messenger = inst.getHostMessenger();
        ZooKeeper zk = messenger.getZK();
        ZKUtil.ByteArrayCallback operationModeFuture = new ZKUtil.ByteArrayCallback();

        // Publish our cluster metadata, and then retrieve the metadata
        // for the rest of the cluster
        try {
            zk.create(VoltZK.cluster_metadata + "/" + messenger.getHostId(),
                      inst.getLocalMetadata().getBytes("UTF-8"),
                      Ids.OPEN_ACL_UNSAFE,
                      CreateMode.EPHEMERAL,
                      new ZKUtil.StringCallback(),
                      null);
            zk.getData(VoltZK.operationMode, false, operationModeFuture, null);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error creating \"/cluster_metadata\" node in ZK", true, e);
        }

        // Spin and attempt to retrieve cluster metadata for all nodes in the cluster.
        Map<Integer, String> clusterMetadata = new HashMap<>(0);
        Set<Integer> metadataToRetrieve = new HashSet<>(messenger.getLiveHostIds());
        metadataToRetrieve.remove(messenger.getHostId());
        while (!metadataToRetrieve.isEmpty()) {
            Map<Integer, ZKUtil.ByteArrayCallback> callbacks = new HashMap<>();
            for (Integer hostId : metadataToRetrieve) {
                ZKUtil.ByteArrayCallback cb = new ZKUtil.ByteArrayCallback();
                zk.getData(VoltZK.cluster_metadata + "/" + hostId, false, cb, null);
                callbacks.put(hostId, cb);
            }

            for (Map.Entry<Integer, ZKUtil.ByteArrayCallback> entry : callbacks.entrySet()) {
                try {
                    ZKUtil.ByteArrayCallback cb = entry.getValue();
                    Integer hostId = entry.getKey();
                    clusterMetadata.put(hostId, new String(cb.get(), "UTF-8"));
                    metadataToRetrieve.remove(hostId);
                } catch (KeeperException.NoNodeException e) {}
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Error retrieving cluster metadata", true, e);
                }
            }
        }

        // print out cluster membership
        hostLog.info("About to list cluster interfaces for all nodes with format [ip1 ip2 ... ipN] client-port,admin-port,http-port");
        for (int hostId : messenger.getLiveHostIds()) {
            String what, meta;
            if (hostId == messenger.getHostId()) {
                what = "SELF";
                meta = inst.getLocalMetadata();
            }
            else {
                what = "PEER";
                meta = clusterMetadata.get(hostId);
            }
            hostLog.infoFmt("  Host id: %d with interfaces: %s [%s]", hostId,
                            MiscUtils.formatHostMetadataFromJSON(meta), what);
        }

        String drRole = catalog.getCluster().getDrrole();
        ProducerDRGateway producerDRGateway = inst.getNodeDRGateway();
        if (producerDRGateway != null && (DrRoleType.MASTER.value().equals(drRole) || DrRoleType.XDCR.value().equals(drRole))) {
            producerDRGateway.logActiveConversations();
        }
        ConsumerDRGateway consumerDRGateway = inst.getConsumerDRGateway();
        if (consumerDRGateway != null) {
            consumerDRGateway.logActiveConversations();
        }

        try {
            byte[] operationMode = operationModeFuture.get();
            if (operationMode != null) {
                OperationMode oldMode = inst.getStartMode();
                OperationMode newMode = OperationMode.valueOf(new String(operationMode, "UTF-8"));
                hostLog.infoFmt("Operation mode changed from %s to %s", oldMode, newMode);
                inst.setStartMode(newMode);
            }
        }
        catch (KeeperException.NoNodeException e) {
            // ignored
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Mainly used by logInfo (above), but may also be called
     * from RealVoltDB on catalog updates.
     *
     * Important: updates the queryTimeout setting in the
     * config as a side effect.
     */
    static void logSystemSettingFromCatalogContext() {
        VoltDBInterface inst = VoltDB.instance();
        Configuration config = inst.getConfig();
        CatalogContext catalog = inst.getCatalogContext();
        if (catalog == null) {
            return;
        }

        Deployment deploy = catalog.cluster.getDeployment().get("deployment");
        Systemsettings sysSettings = deploy.getSystemsettings().get("systemsettings");
        if (sysSettings == null) {
            return;
        }

        hostLog.infoFmt("Elastic duration set to %s milliseconds", sysSettings.getElasticduration());
        hostLog.infoFmt("Elastic throughput set to %s MB/s", sysSettings.getElasticthroughput());
        hostLog.infoFmt("Max temptable size set to %s MB", sysSettings.getTemptablemaxsize());
        hostLog.infoFmt("Snapshot priority set to %s (if > 0, delays snapshot tasks)", sysSettings.getSnapshotpriority());

        int queryTmo = sysSettings.getQuerytimeout();
        if (queryTmo > 0) {
            hostLog.infoFmt("Query timeout set to %s milliseconds", queryTmo);
            config.m_queryTimeout = queryTmo;
        }
        else if (queryTmo == 0) {
            hostLog.info("Query timeout set to unlimited");
            config.m_queryTimeout = 0;
        }
    }
}
