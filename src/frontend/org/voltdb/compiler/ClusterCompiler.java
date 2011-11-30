/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Site;

public class ClusterCompiler
{
    /**
     * Add hosts, sites, and partitions to the catalog
     * @param catalog  The catalog to be modified
     * @param clusterConfig  The desired cluster configuration
     */
    public static void compile(Catalog catalog, ClusterConfig clusterConfig)
    {
        if (!clusterConfig.validate())
        {
            throw new RuntimeException(clusterConfig.getErrorMsg());
        }

        int hostCount = clusterConfig.getHostCount();
        int partitionCount = clusterConfig.getPartitionCount();
        int sitesPerHost = clusterConfig.getSitesPerHost();

        // add all the hosts
        Cluster cluster = catalog.getClusters().get("cluster");
        for (int i = 0; i < hostCount; i++) {
            cluster.getHosts().add(String.valueOf(i));
        }

        // add all the partitions.
        for (int i = 0; i < partitionCount; ++i) {
            cluster.getPartitions().add(String.valueOf(i));
        }

        // add all the sites
        int initiatorsPerHost = 1;
        int partitionCounter = -1;
        int nextInitiatorId = 1;
        for (int i = 0; i < (sitesPerHost + initiatorsPerHost) * hostCount; i++) {

            int hostForSite = i / (sitesPerHost + initiatorsPerHost);
            Host host = cluster.getHosts().get(String.valueOf(hostForSite));
            int hostId = Integer.parseInt(host.getTypeName());

            int withinHostId = i % (sitesPerHost + initiatorsPerHost);

            int siteId = hostId * VoltDB.SITES_TO_HOST_DIVISOR + withinHostId;

            Site site = cluster.getSites().add(String.valueOf(siteId));

            if (withinHostId >= initiatorsPerHost) {
                site.setIsexec(true);
                // serially assign partitions to execution sites.
                site.setPartition(cluster.getPartitions().get(
                        String.valueOf((++partitionCounter) % partitionCount)));
            }
            else {
                site.setInitiatorid(nextInitiatorId++);
                site.setIsexec(false);
            }
            site.setHost(host);
            site.setIsup(true);
        }
    }
}
