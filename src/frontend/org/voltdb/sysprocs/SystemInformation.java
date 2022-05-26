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

package org.voltdb.sysprocs;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketHubAppender;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.common.Constants;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.CoreZK;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.CommandLog;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Priorities;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.User;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.VoltTableUtil;

/**
 * Access key/value tables of cluster info that correspond to the REST
 * API members/properties
 */
public class SystemInformation extends VoltSystemProcedure
{
    public static final String DR_PUBLIC_INTF_COL = "DRPUBLICINTERFACE";
    public static final String DR_PUBLIC_PORT_COL = "DRPUBLICPORT";
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final ColumnInfo clusterInfoSchema[] = new ColumnInfo[]
    {
        new ColumnInfo("PROPERTY", VoltType.STRING),
        new ColumnInfo("VALUE", VoltType.STRING)
    };

    @Override
    public long[] getPlanFragmentIds()
    {
        return new long[]{
            SysProcFragmentId.PF_systemInformationOverview,
            SysProcFragmentId.PF_systemInformationOverviewAggregate,
            SysProcFragmentId.PF_systemInformationDeployment,
            SysProcFragmentId.PF_systemInformationAggregate,
            SysProcFragmentId.PF_systemInformationLicense
        };
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies,
                                              long fragmentId,
                                              ParameterSet params,
                                              SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_systemInformationOverview)
        {
            VoltTable result = null;
            // Choose the lowest site ID on this host to do the info gathering
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                result = populateOverviewTable();
            }
            else
            {
                result = new VoltTable(
                                       new ColumnInfo(CNAME_HOST_ID, CTYPE_ID),
                                       new ColumnInfo("KEY", VoltType.STRING),
                                       new ColumnInfo("VALUE", VoltType.STRING));
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_systemInformationOverview, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationOverviewAggregate)
        {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_systemInformationOverview));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_systemInformationOverviewAggregate, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationDeployment)
        {
            VoltTable result = null;
            // Choose the lowest site ID on this host to do the info gathering
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                result = populateDeploymentProperties(context.getCluster(),
                        context.getDatabase(), m_clusterSettings, m_nodeSettings);
            }
            else
            {
                result = new VoltTable(clusterInfoSchema);
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_systemInformationDeployment, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationAggregate)
        {
            VoltTable result = null;
            // Check for KEY/VALUE consistency
            List<VoltTable> answers =
                dependencies.get(SysProcFragmentId.PF_systemInformationDeployment);
            for (VoltTable answer : answers)
            {
                // if we got an empty table from a non-lowest execution site ID,
                // ignore it
                if (answer.getRowCount() == 0)
                {
                    continue;
                }
                // Save the first real answer we got and compare all future
                // answers with it
                if (result == null)
                {
                    result = answer;
                }
                else
                {
                    if (!verifyReturnedTablesMatch(answer, result))
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Inconsistent results returned for @SystemInformation: \n");
                        sb.append("Result #1: ");
                        sb.append(result.toString());
                        sb.append("\n");
                        sb.append("Result #2: ");
                        sb.append(answer.toString());
                        sb.append("\n");
                        hostLog.error(sb.toString());
                        throw new VoltAbortException(sb.toString());
                    }
                }
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_systemInformationAggregate, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationLicense)
        {
            VoltTable result = null;
            // Choose the lowest site ID on this host to do the info gathering
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                result = populateLicenseProperties();
            }
            else
            {
                result = new VoltTable(clusterInfoSchema);
            }
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_systemInformationLicense, result);
        }
        assert(false);
        return null;
    }

    boolean verifyReturnedTablesMatch(VoltTable first, VoltTable second)
    {
        boolean retval = true;
        HashMap<String, String> first_map = new HashMap<String, String>();
        HashMap<String, String> second_map = new HashMap<String, String>();

        try {
            while (first.advanceRow())
            {
                first_map.put(first.getString(0), first.getString(1));
            }
            while (second.advanceRow())
            {
                second_map.put(second.getString(0), second.getString(1));
            }

            if (first_map.size() != second_map.size())
            {
                retval = false;
            }
            else
            {
                for (Entry<String, String> first_entry : first_map.entrySet())
                {
                    String second_value = second_map.get(first_entry.getKey());
                    if (second_value == null || !second_value.equals(first_entry.getValue()))
                    {
                        // Ignore deltas due to LocalCluster's use of
                        // VoltSnapshotFilePrefix
                        if ((((first_entry.getKey())).contains("path") ||
                                ((first_entry.getKey())).contains("root")) &&
                                (System.getProperty("VoltSnapshotFilePrefix") != null))
                        {
                            continue;
                        }
                        else
                        {
                            retval = false;
                            break;
                        }
                    }
                }
            }
        } finally {
            if (first != null) {
                first.resetRowPosition();
            }
            if (second != null) {
                second.resetRowPosition();
            }
        }
        return retval;
    }

    /**
     * Returns the cluster info requested by the provided selector
     * @param ctx          Internal. Not exposed to the end-user.
     * @param selector     Selector requested
     * @return             The property/value table for the provided selector
     * @throws VoltAbortException
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String selector) throws VoltAbortException
    {
        VoltTable[] results;

        // This selector provides the old @SystemInformation behavior
        if ("OVERVIEW".equalsIgnoreCase(selector.toString()))
        {
            results = getOverviewInfo();
        }
        else if ("DEPLOYMENT".equalsIgnoreCase(selector.toString()))
        {
            results = getDeploymentInfo();
        }
        else if ("LICENSE".equalsIgnoreCase(selector.toString()))
        {
            results = getLicenseInfo();
        }
        else
        {
            throw new VoltAbortException(String.format("Invalid @SystemInformation selector %s.", selector));
        }

        return results;
    }

    /**
     * Retrieve basic management information about the cluster.
     * Use this procedure to read the ipaddress, hostname, buildstring
     * and version of each node of the cluster.
     *
     * @return          A table with three columns:
     *  HOST_ID(INTEGER), KEY(STRING), VALUE(STRING).
     */
    private VoltTable[] getOverviewInfo()
    {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_systemInformationOverview,
                SysProcFragmentId.PF_systemInformationOverviewAggregate);
    }

    private VoltTable[] getDeploymentInfo() {
        // create a work fragment to gather deployment data from each of the sites.
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_systemInformationDeployment,
                SysProcFragmentId.PF_systemInformationAggregate);
    }
    private VoltTable[] getLicenseInfo() {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_systemInformationLicense,
                SysProcFragmentId.PF_systemInformationAggregate);
    }

    public static VoltTable constructOverviewTable() {
        return new VoltTable(
                new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID,
                        VoltSystemProcedure.CTYPE_ID),
         new ColumnInfo("KEY", VoltType.STRING),
         new ColumnInfo("VALUE", VoltType.STRING));
    }

    /**
     * Accumulate per-host information and return as a table.
     * This function does the real work. Everything else is
     * boilerplate sysproc stuff.
     */
    static public VoltTable populateOverviewTable()
    {
        VoltTable vt = constructOverviewTable();
        int hostId = VoltDB.instance().getHostMessenger().getHostId();

        // try to get the external interface first, if none was set, use local addresses
        InetAddress addr = null;
        String clientInterface = null;
        int clientPort = VoltDB.DEFAULT_PORT;
        String adminInterface = null;
        int adminPort = VoltDB.DEFAULT_ADMIN_PORT;
        String httpInterface = null;
        int httpPort = VoltDB.DEFAULT_HTTP_PORT;
        String internalInterface = null;
        int internalPort = Constants.DEFAULT_INTERNAL_PORT;
        String zkInterface = null;
        int zkPort = Constants.DEFAULT_ZK_PORT;
        String drInterface = null;
        int drPort = VoltDB.DEFAULT_DR_PORT;
        String publicInterface = null;
        String drPublicInterface = null;
        int drPublicPort = 0;
        String topicsPublicInterface = null;
        int topicsPublicPort = 0;
        int topicsPort = VoltDB.DEFAULT_TOPICS_PORT;
        try {
            String localMetadata = VoltDB.instance().getLocalMetadata();
            JSONObject jsObj = new JSONObject(localMetadata);
            JSONArray interfaces = jsObj.getJSONArray("interfaces");
            String iface = interfaces.getString(0);
            addr = InetAddress.getByName(iface);
            clientPort = jsObj.getInt("clientPort");
            clientInterface = jsObj.getString("clientInterface");
            adminPort = jsObj.getInt("adminPort");
            adminInterface = jsObj.getString("adminInterface");
            httpPort = jsObj.getInt("httpPort");
            httpInterface = jsObj.getString("httpInterface");
            internalPort = jsObj.getInt("internalPort");
            internalInterface = jsObj.getString("internalInterface");
            zkPort = jsObj.getInt("zkPort");
            zkInterface = jsObj.getString("zkInterface");
            drPort = jsObj.getInt("drPort");
            drInterface = jsObj.getString("drInterface");
            publicInterface = jsObj.getString("publicInterface");
            drPublicInterface = jsObj.getString(VoltZK.drPublicHostProp);
            drPublicPort = jsObj.getInt(VoltZK.drPublicPortProp);
            topicsPublicInterface = jsObj.getString("topicsPublicHost");
            topicsPublicPort = jsObj.getInt("topicsPublicPort");
            topicsPort = jsObj.getInt("topicsport");
        } catch (JSONException e) {
            hostLog.info("Failed to get local metadata, falling back to first resolvable IP address.");
        } catch (UnknownHostException e) {
            hostLog.info("Failed to determine hostname, falling back to first resolvable IP address.");
        }

        // host name and IP address.
        if (addr == null) {
            addr = org.voltcore.utils.CoreUtils.getLocalAddress();
        }
        vt.addRow(hostId, "IPADDRESS", addr.getHostAddress());
        vt.addRow(hostId, "HOSTNAME", CoreUtils.getHostnameOrAddress());
        vt.addRow(hostId, "CLIENTINTERFACE", clientInterface);
        vt.addRow(hostId, "CLIENTPORT", Integer.toString(clientPort));
        vt.addRow(hostId, "ADMININTERFACE", adminInterface);
        vt.addRow(hostId, "ADMINPORT", Integer.toString(adminPort));
        vt.addRow(hostId, "HTTPINTERFACE", httpInterface);
        vt.addRow(hostId, "HTTPPORT", Integer.toString(httpPort));
        vt.addRow(hostId, "INTERNALINTERFACE", internalInterface);
        vt.addRow(hostId, "INTERNALPORT", Integer.toString(internalPort));
        vt.addRow(hostId, "ZKINTERFACE", zkInterface);
        vt.addRow(hostId, "ZKPORT", Integer.toString(zkPort));
        vt.addRow(hostId, "DRINTERFACE", drInterface);
        vt.addRow(hostId, "DRPORT", Integer.toString(drPort));
        vt.addRow(hostId, "PUBLICINTERFACE", publicInterface);
        vt.addRow(hostId, DR_PUBLIC_INTF_COL, drPublicInterface);
        vt.addRow(hostId, DR_PUBLIC_PORT_COL, Integer.toString(drPublicPort));

        // build string
        vt.addRow(hostId, "BUILDSTRING", VoltDB.instance().getBuildString());

        // version
        vt.addRow(hostId, "VERSION", VoltDB.instance().getVersionString());
        // catalog path
        String path = VoltDB.instance().getConfig().m_pathToCatalog;
        if (path != null && !path.startsWith("http")) {
            path = (new File(path)).getAbsolutePath();
        }
        vt.addRow(hostId, "CATALOG", path);

        // deployment path
        path = VoltDB.instance().getConfig().m_pathToDeployment;
        if (path != null && !path.startsWith("http")) {
            path = (new File(path)).getAbsolutePath();
        }
        vt.addRow(hostId, "DEPLOYMENT", path);

        String cluster_state = VoltDB.instance().getMode().toString();
        vt.addRow(hostId, "CLUSTERSTATE", cluster_state);

        vt.addRow(hostId, "CLUSTERSAFETY", VoltDB.instance().isMasterOnly() ? "REDUCED" : "FULL");

        // INITIALIZED, used by VEM to determine the spinny icon state.
        org.voltdb.OperationMode mode = VoltDB.instance().getMode();
        String areInitialized = Boolean.toString(!VoltDB.instance().rejoining() &&
                (mode == org.voltdb.OperationMode.RUNNING ||
                        mode == org.voltdb.OperationMode.PAUSED));
        vt.addRow(hostId, "INITIALIZED", areInitialized);

        String replication_role = VoltDB.instance().getReplicationRole().toString();
        vt.addRow(hostId, "REPLICATIONROLE", replication_role);

        vt.addRow(hostId, "CATALOGCRC",
                Long.toString(VoltDB.instance().getCatalogContext().getCatalogCRC()));

        vt.addRow(hostId, "IV2ENABLED", "true");
        long startTimeMs = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        vt.addRow(hostId, "STARTTIME", Long.toString(startTimeMs));
        long createTimeMs = VoltDB.instance().getClusterCreateTime();
        vt.addRow(hostId, "CREATIONTIME", Long.toString(createTimeMs));
        long hostStartTimeMs = VoltDB.instance().getHostStartTime().toEpochMilli();
        vt.addRow(hostId, "HOSTSTARTTIME", Long.toString(hostStartTimeMs));
        vt.addRow(hostId, "UPTIME", MiscUtils.formatUptime(VoltDB.instance().getClusterUptime()));

        vt.addRow(hostId, "LAST_UPDATECORE_DURATION",
                Long.toString(VoltDB.instance().getCatalogContext().m_lastUpdateCoreDuration));

        SocketHubAppender hubAppender =
            (SocketHubAppender) Logger.getRootLogger().getAppender("hub");
        int port = 0;
        if (hubAppender != null) {
            port = hubAppender.getPort();
        }
        vt.addRow(hostId, "LOG4JPORT", Integer.toString(port));
        //Add license information
        if (MiscUtils.isPro()) {
            vt.addRow(hostId, "LICENSE", VoltDB.instance().getLicensing().getLicenseSummary());
        }
        populatePartitionGroups(hostId, vt);

        // root path
        vt.addRow(hostId, "VOLTDBROOT", VoltDB.instance().getVoltDBRootPath());
        vt.addRow(hostId, "FULLCLUSTERSIZE", Integer.toString(VoltDB.instance().getCatalogContext().getClusterSettings().hostcount()));
        vt.addRow(hostId, "CLUSTERID", Integer.toString(VoltDB.instance().getCatalogContext().getCluster().getDrclusterid()));
        vt.addRow(hostId, "TOPICSPUBLICINTERFACE", topicsPublicInterface);
        vt.addRow(hostId, "TOPICSPUBLICPORT", Integer.toString(topicsPublicPort));
        vt.addRow(hostId, "TOPICSPORT", Integer.toString(topicsPort));

        // platform properties
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        vt.addRow(hostId, "CPUCORES", Integer.toString(pp.coreCount));
        vt.addRow(hostId, "CPUTHREADS", Integer.toString(pp.hardwareThreads));
        vt.addRow(hostId, "MEMORY", Integer.toString(pp.ramInMegabytes) + " MB");
        vt.addRow(hostId, "KUBERNETES", Boolean.toString(VoltDB.instance().getConfig().runningUnderKubernetes()));

        return vt;
    }

    private static void populatePartitionGroups(Integer hostId, VoltTable vt) {
        try {
            byte[]  bytes = VoltDB.instance().getHostMessenger().getZK().getData(CoreZK.hosts_host + hostId, false, new Stat());
            String hostInfo = new String(bytes, StandardCharsets.UTF_8);
            JSONObject obj = new JSONObject(hostInfo);
            vt.addRow(hostId, "PLACEMENTGROUP",obj.getString("group"));
        } catch (KeeperException | InterruptedException | JSONException e) {
            vt.addRow(hostId, "PLACEMENTGROUP","NULL");
        }
        Set<Integer> buddies = VoltDB.instance().getCartographer().getHostIdsWithinPartitionGroup(hostId);
        String[] strIds = buddies.stream().sorted().map(String::valueOf).toArray(String[]::new);
        vt.addRow(hostId, "PARTITIONGROUP",String.join(",", strIds));
    }

    static public VoltTable populateDeploymentProperties(
            Cluster cluster,
            Database database,
            ClusterSettings clusterSettings,
            NodeSettings nodeSettings)
    {
        VoltDBInterface voltdb = VoltDB.instance();
        VoltTable results = new VoltTable(clusterInfoSchema);
        // it would be awesome if these property names could come
        // from the RestApiDescription.xml (or the equivalent thereof) someday --izzy
        results.addRow("voltdbroot", voltdb.getVoltDBRootPath());

        Deployment deploy = cluster.getDeployment().get("deployment");
        results.addRow("hostcount", Integer.toString(clusterSettings.hostcount()));
        results.addRow("kfactor", Integer.toString(deploy.getKfactor()));
        results.addRow("sitesperhost", Integer.toString(nodeSettings.getLocalSitesCount()));

        String http_enabled = "false";
        int http_port = voltdb.getConfig().m_httpPort;
        if (http_port != -1 && http_port != Integer.MAX_VALUE) {
            http_enabled = "true";
            results.addRow("httpport", Integer.toString(http_port));
        }
        results.addRow("httpenabled", http_enabled);

        String json_enabled = "false";
        if (cluster.getJsonapi())
        {
            json_enabled = "true";
        }
        results.addRow("jsonenabled", json_enabled);

        results.addRow("snapshotpath", voltdb.getSnapshotPath());
        SnapshotSchedule snaps = database.getSnapshotschedule().get("default");
        String autosnap_enabled = "false";
        if (snaps != null && snaps.getEnabled())
        {
            autosnap_enabled = "true";
            String snap_freq = Integer.toString(snaps.getFrequencyvalue()) + snaps.getFrequencyunit();
            results.addRow("snapshotprefix", snaps.getPrefix());
            results.addRow("snapshotfrequency", snap_freq);
            results.addRow("snapshotretain", Integer.toString(snaps.getRetain()));
        }
        results.addRow("snapshotenabled", autosnap_enabled);

        for (Connector export_conn : database.getConnectors()) {
            if (export_conn != null && export_conn.getEnabled())
            {
                results.addRow("exportoverflowpath", voltdb.getExportOverflowPath().getPath());
                results.addRow("exportcursorpath", voltdb.getExportCursorPath());
                break;
            }
        }

        results.addRow("export", Boolean.toString(CatalogUtil.isExportEnabled()));

        if (cluster.getDrproducerenabled() || cluster.getDrconsumerenabled()) {
            results.addRow("droverflowpath", voltdb.getDROverflowPath());
        }
        results.addRow("largequeryswappath", voltdb.getLargeQuerySwapPath());

        String partition_detect_enabled = "false";
        if (cluster.getNetworkpartition())
        {
            partition_detect_enabled = "true";
        }
        results.addRow("partitiondetection", partition_detect_enabled);

        results.addRow("heartbeattimeout", Integer.toString(cluster.getHeartbeattimeout()));
        results.addRow("adminport", Integer.toString(voltdb.getConfig().m_adminPort));

        String command_log_enabled = "false";
        // log name is MAGIC, you knoooow
        CommandLog command_log = cluster.getLogconfig().get("log");
        if (command_log.getEnabled())
        {
            command_log_enabled = "true";
            String command_log_mode = "async";
            if (command_log.getSynchronous())
            {
                command_log_mode = "sync";
            }
            String command_log_path = voltdb.getCommandLogPath();
            String command_log_snaps = voltdb.getCommandLogSnapshotPath();
            String command_log_fsync_interval =
                Integer.toString(command_log.getFsyncinterval());
            String command_log_max_txns =
                Integer.toString(command_log.getMaxtxns());
            results.addRow("commandlogmode", command_log_mode);
            results.addRow("commandlogfreqtime", command_log_fsync_interval);
            results.addRow("commandlogfreqtxns", command_log_max_txns);
            results.addRow("commandlogpath", command_log_path);
            results.addRow("commandlogsnapshotpath", command_log_snaps);
        }
        results.addRow("commandlogenabled", command_log_enabled);

        String users = "";
        for (User user : database.getUsers())
        {
            users += addEscapes(user.getTypeName());
            if (user.getGroups() != null && user.getGroups().size() > 0)
            {
                users += ":";
                for (GroupRef gref : user.getGroups())
                {
                    users += addEscapes(gref.getGroup().getTypeName());
                    users += ",";
                }
                users = users.substring(0, users.length() - 1);
            }
            users += ";";
        }
        results.addRow("users", users);

        // Add system setting information also
        // the attribute names follows the above naming rule
        Systemsettings sysSettings = deploy.getSystemsettings().get("systemsettings");
        Priorities priorities = sysSettings.getPriorities().get("priorities");

        results.addRow("elasticduration", Integer.toString(sysSettings.getElasticduration()));
        results.addRow("elasticthroughput", Integer.toString(sysSettings.getElasticthroughput()));
        results.addRow("snapshotpriority", priorities.getEnabled() ?
                Integer.toString(priorities.getSnapshotpriority()) : Integer.toString(sysSettings.getSnapshotpriority()));
        results.addRow("temptablesmaxsize", Integer.toString(sysSettings.getTemptablemaxsize()));
        results.addRow("querytimeout", Integer.toString(sysSettings.getQuerytimeout()));

        // Add xdcr related information
        if (cluster.getDrrole() != null) {
            results.addRow("drrole", cluster.getDrrole());
        }

        // Add priorities information
        results.addRow("prioritiesenabled", Boolean.toString(priorities.getEnabled()));
        results.addRow("drpriority", priorities.getEnabled() ?
                Integer.toString(priorities.getDrpriority()) : "null");
        return results;
    }

    static private String addEscapes(String name)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++)
        {
            if (name.charAt(i) == ';' || name.charAt(i) == ':' || name.charAt(i) == '\\')
            {
                sb.append('\\');
            }
            sb.append(name.charAt(i));
        }

        return sb.toString();
    }

    static public VoltTable populateLicenseProperties()
    {
        VoltTable results = new VoltTable(clusterInfoSchema);
        VoltDB.instance().getLicensing().populateLicenseInfo(results);
        return results;
    }
}
