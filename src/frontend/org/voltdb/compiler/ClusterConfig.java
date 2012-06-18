/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
package org.voltdb.compiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;

public class ClusterConfig
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static List<Integer> partitionsForHost(JSONObject topo, int hostId) throws JSONException
    {
        List<Integer> partitions = new ArrayList<Integer>();

        JSONArray parts = topo.getJSONArray("partitions");

        for (int p = 0; p < parts.length(); p++) {
            // have an object in the partitions array
            JSONObject aPartition = parts.getJSONObject(p);
            int pid = aPartition.getInt("partition_id");
            JSONArray replicas = aPartition.getJSONArray("replicas");
            for (int h = 0; h < replicas.length(); h++)
            {
                int replica = replicas.getInt(h);
                if (replica == hostId)
                {
                    partitions.add(pid);
                }
            }
        }

        return partitions;
    }

    public ClusterConfig(int hostCount, int sitesPerHost, int replicationFactor)
    {
        m_hostCount = hostCount;
        m_sitesPerHost = sitesPerHost;
        m_replicationFactor = replicationFactor;
        m_errorMsg = "Config is unvalidated";
    }

    public int getHostCount()
    {
        return m_hostCount;
    }

    public int getSitesPerHost()
    {
        return m_sitesPerHost;
    }

    public int getReplicationFactor()
    {
        return m_replicationFactor;
    }

    public int getPartitionCount()
    {
        return (m_hostCount * m_sitesPerHost) / (m_replicationFactor + 1);
    }

    public String getErrorMsg()
    {
        return m_errorMsg;
    }

    public boolean validate()
    {
        if (m_hostCount <= 0)
        {
            m_errorMsg = "The number of hosts must be > 0.";
            return false;
        }
        if (m_sitesPerHost <= 0)
        {
            m_errorMsg = "The number of sites per host must be > 0.";
            return false;
        }
        if (m_hostCount <= m_replicationFactor)
        {
            m_errorMsg = String.format("%d servers required for K-safety=%d",
                                       m_replicationFactor + 1, m_replicationFactor);
            return false;
        }
        if (getPartitionCount() == 0)
        {
            m_errorMsg = String.format("Insufficient execution site count to achieve K-safety of %d",
                                       m_replicationFactor);
            return false;
        }
        m_errorMsg = "Cluster config contains no detected errors";
        return true;
    }

    // Statically build a topology. This only runs at startup;
    // rejoin clones this from an existing server.
    public JSONObject getTopology(List<Integer> hostIds) throws JSONException
    {
        int hostCount = getHostCount();
        int partitionCount = getPartitionCount();
        int sitesPerHost = getSitesPerHost();

        // add all the sites
        int partitionCounter = -1;

        HashMap<Integer, ArrayList<Integer>> partToHosts =
            new HashMap<Integer, ArrayList<Integer>>();
        for (int i = 0; i < partitionCount; i++)
        {
            ArrayList<Integer> hosts = new ArrayList<Integer>();
            partToHosts.put(i, hosts);
        }
        for (int i = 0; i < sitesPerHost * hostCount; i++) {

            // serially assign partitions to execution sites.
            int partition = (++partitionCounter) % partitionCount;
            int hostForSite = hostIds.get(i / sitesPerHost);
            partToHosts.get(partition).add(hostForSite);
        }

        // {"kfactor" : 2,
        //  "sites_per_host" : 3,
        //  "partitions" :
        //    [{"partition_id" : 0,
        //      "replicas" : [hostid1, hostid2, hostid3]},
        //     {"partition_id" : 1,
        //      "replicas" : [hostid1, hostid2, hostid3]}
        //    ]
        // }

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("hostcount").value(m_hostCount);
        stringer.key("kfactor").value(getReplicationFactor());
        stringer.key("sites_per_host").value(sitesPerHost);
        stringer.key("partitions").array();
        for (int part = 0; part < partitionCount; part++)
        {
            stringer.object();
            stringer.key("partition_id").value(part);
            stringer.key("replicas").array();
            for (int host_pos : partToHosts.get(part))
            {
                stringer.value(host_pos);
            }
            stringer.endArray();
            stringer.endObject();
        }
        stringer.endArray();
        stringer.endObject();

        JSONObject topo = new JSONObject(stringer.toString());
        hostLog.debug("TOPO: " + topo.toString(2));

        return topo;
    }

    private final int m_hostCount;
    private final int m_sitesPerHost;
    private final int m_replicationFactor;

    private String m_errorMsg;
}
