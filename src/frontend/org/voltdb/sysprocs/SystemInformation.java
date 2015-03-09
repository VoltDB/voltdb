/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketHubAppender;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.CommandLog;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.User;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;

/**
 * Access key/value tables of cluster info that correspond to the REST
 * API members/properties
 */
@ProcInfo(
    singlePartition = false
)

public class SystemInformation extends VoltSystemProcedure
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    static final int DEP_DISTRIBUTE = (int)
        SysProcFragmentId.PF_systemInformationOverview | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_AGGREGATE = (int) SysProcFragmentId.PF_systemInformationOverviewAggregate;

    static final int DEP_systemInformationDeployment = (int)
        SysProcFragmentId.PF_systemInformationDeployment | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_systemInformationAggregate = (int)
        SysProcFragmentId.PF_systemInformationAggregate;

    public static final ColumnInfo clusterInfoSchema[] = new ColumnInfo[]
    {
        new ColumnInfo("PROPERTY", VoltType.STRING),
        new ColumnInfo("VALUE", VoltType.STRING)
    };

    @Override
    public void init()
    {
        registerPlanFragment(SysProcFragmentId.PF_systemInformationOverview);
        registerPlanFragment(SysProcFragmentId.PF_systemInformationOverviewAggregate);
        registerPlanFragment(SysProcFragmentId.PF_systemInformationDeployment);
        registerPlanFragment(SysProcFragmentId.PF_systemInformationAggregate);
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
            return new DependencyPair(DEP_DISTRIBUTE, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationOverviewAggregate)
        {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_DISTRIBUTE));
            return new DependencyPair(DEP_AGGREGATE, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationDeployment)
        {
            VoltTable result = null;
            // Choose the lowest site ID on this host to do the info gathering
            // All other sites should just return empty results tables.
            if (context.isLowestSiteId())
            {
                result = populateDeploymentProperties(context.getCluster(), context.getDatabase());
            }
            else
            {
                result = new VoltTable(clusterInfoSchema);
            }
            return new DependencyPair(DEP_systemInformationDeployment, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_systemInformationAggregate)
        {
            VoltTable result = null;
            // Check for KEY/VALUE consistency
            List<VoltTable> answers =
                dependencies.get(DEP_systemInformationDeployment);
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
            return new DependencyPair(DEP_systemInformationAggregate, result);
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
                        // VoltFileRoot
                        if ((((first_entry.getKey())).contains("path") ||
                                ((first_entry.getKey())).contains("root")) &&
                                (System.getProperty("VoltFilePrefix") != null))
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
        if (selector.toUpperCase().equals("OVERVIEW"))
        {
            results = getOverviewInfo();
        }
        else if (selector.toUpperCase().equals("DEPLOYMENT"))
        {
            results = getDeploymentInfo();
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
        SynthesizedPlanFragment spf[] = new SynthesizedPlanFragment[2];
        spf[0] = new SynthesizedPlanFragment();
        spf[0].fragmentId = SysProcFragmentId.PF_systemInformationOverview;
        spf[0].outputDepId = DEP_DISTRIBUTE;
        spf[0].inputDepIds = new int[] {};
        spf[0].multipartition = true;
        spf[0].parameters = ParameterSet.emptyParameterSet();

        spf[1] = new SynthesizedPlanFragment();
        spf[1] = new SynthesizedPlanFragment();
        spf[1].fragmentId = SysProcFragmentId.PF_systemInformationOverviewAggregate;
        spf[1].outputDepId = DEP_AGGREGATE;
        spf[1].inputDepIds = new int[] { DEP_DISTRIBUTE };
        spf[1].multipartition = false;
        spf[1].parameters = ParameterSet.emptyParameterSet();

        return executeSysProcPlanFragments(spf, DEP_AGGREGATE);
    }

    private VoltTable[] getDeploymentInfo() {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather deployment data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_systemInformationDeployment;
        pfs[1].outputDepId = DEP_systemInformationDeployment;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        // create a work fragment to aggregate the results.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_systemInformationAggregate;
        pfs[0].outputDepId = DEP_systemInformationAggregate;
        pfs[0].inputDepIds = new int[]{DEP_systemInformationDeployment};
        pfs[0].multipartition = false;
        pfs[0].parameters = ParameterSet.emptyParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_systemInformationAggregate);
        return results;
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
        int internalPort = VoltDB.DEFAULT_INTERNAL_PORT;
        String zkInterface = null;
        int zkPort = VoltDB.DEFAULT_ZK_PORT;
        String drInterface = null;
        int drPort = VoltDB.DEFAULT_DR_PORT;
        String publicInterface = null;
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

        // build string
        vt.addRow(hostId, "BUILDSTRING", VoltDB.instance().getBuildString());

        // version
        vt.addRow(hostId, "VERSION", VoltDB.instance().getVersionString());
        // catalog path
        String path = VoltDB.instance().getConfig().m_pathToCatalog;
        if (path != null && !path.startsWith("http"))
            path = (new File(path)).getAbsolutePath();
        vt.addRow(hostId, "CATALOG", path);

        // deployment path
        path = VoltDB.instance().getConfig().m_pathToDeployment;
        if (path != null && !path.startsWith("http"))
            path = (new File(path)).getAbsolutePath();
        vt.addRow(hostId, "DEPLOYMENT", path);

        String cluster_state = VoltDB.instance().getMode().toString();
        vt.addRow(hostId, "CLUSTERSTATE", cluster_state);
        // INITIALIZED, used by VEM to determine the spinny icon state.
        org.voltdb.OperationMode mode = VoltDB.instance().getMode();
        String areInitialized = Boolean.toString(!VoltDB.instance().rejoining() &&
                (mode == org.voltdb.OperationMode.RUNNING ||
                        mode == org.voltdb.OperationMode.PAUSED));
        vt.addRow(hostId, "INITIALIZED", areInitialized);

        String replication_role = VoltDB.instance().getReplicationRole().toString();
        vt.addRow(hostId, "REPLICATIONROLE", replication_role);

        vt.addRow(hostId, "LASTCATALOGUPDATETXNID",
                  Long.toString(VoltDB.instance().getCatalogContext().m_transactionId));
        vt.addRow(hostId, "CATALOGCRC",
                Long.toString(VoltDB.instance().getCatalogContext().getCatalogCRC()));

        vt.addRow(hostId, "IV2ENABLED", "true");
        long startTimeMs = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        vt.addRow(hostId, "STARTTIME", Long.toString(startTimeMs));
        vt.addRow(hostId, "UPTIME", MiscUtils.formatUptime(VoltDB.instance().getClusterUptime()));

        SocketHubAppender hubAppender =
            (SocketHubAppender) Logger.getRootLogger().getAppender("hub");
        int port = 0;
        if (hubAppender != null)
            port = hubAppender.getPort();
        vt.addRow(hostId, "LOG4JPORT", Integer.toString(port));

        return vt;
    }

    static public VoltTable populateDeploymentProperties(Cluster cluster, Database database)
    {
        VoltTable results = new VoltTable(clusterInfoSchema);
        // it would be awesome if these property names could come
        // from the RestApiDescription.xml (or the equivalent thereof) someday --izzy
        results.addRow("voltdbroot", cluster.getVoltroot());

        Deployment deploy = cluster.getDeployment().get("deployment");
        results.addRow("hostcount", Integer.toString(deploy.getHostcount()));
        results.addRow("kfactor", Integer.toString(deploy.getKfactor()));
        results.addRow("sitesperhost", Integer.toString(deploy.getSitesperhost()));

        String http_enabled = "false";
        int http_port = VoltDB.instance().getConfig().m_httpPort;
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

        SnapshotSchedule snaps = database.getSnapshotschedule().get("default");
        String snap_enabled = "false";
        if (snaps != null && snaps.getEnabled())
        {
            snap_enabled = "true";
            String snap_freq = Integer.toString(snaps.getFrequencyvalue()) + snaps.getFrequencyunit();
            results.addRow("snapshotpath", snaps.getPath());
            results.addRow("snapshotprefix", snaps.getPrefix());
            results.addRow("snapshotfrequency", snap_freq);
            results.addRow("snapshotretain", Integer.toString(snaps.getRetain()));
        }
        results.addRow("snapshotenabled", snap_enabled);

        String export_enabled = "false";
        for (Connector export_conn : database.getConnectors()) {
            if (export_conn != null && export_conn.getEnabled())
            {
                export_enabled = "true";
                results.addRow("exportoverflowpath", cluster.getExportoverflow());
                break;
            }
        }
        results.addRow("export", export_enabled);

        String partition_detect_enabled = "false";
        if (cluster.getNetworkpartition())
        {
            partition_detect_enabled = "true";
            String partition_detect_snapshot_path =
                cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPath();
            String partition_detect_snapshot_prefix =
                cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix();
            results.addRow("snapshotpath",
                           partition_detect_snapshot_path);
            results.addRow("partitiondetectionsnapshotprefix",
                           partition_detect_snapshot_prefix);
        }
        results.addRow("partitiondetection", partition_detect_enabled);

        results.addRow("heartbeattimeout", Integer.toString(cluster.getHeartbeattimeout()));

        results.addRow("adminport", Integer.toString(VoltDB.instance().getConfig().m_adminPort));
        String adminstartup = "false";
        if (cluster.getAdminstartup())
        {
            adminstartup = "true";
        }
        results.addRow("adminstartup", adminstartup);

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
            String command_log_path = command_log.getLogpath();
            String command_log_snaps = command_log.getInternalsnapshotpath();
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
        results.addRow("elasticduration", Integer.toString(sysSettings.getElasticduration()));
        results.addRow("elasticthroughput", Integer.toString(sysSettings.getElasticthroughput()));
        results.addRow("snapshotpriority", Integer.toString(sysSettings.getSnapshotpriority()));
        results.addRow("temptablesmaxsize", Integer.toString(sysSettings.getTemptablemaxsize()));
        results.addRow("querytimeout", Integer.toString(sysSettings.getQuerytimeout()));

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
}
