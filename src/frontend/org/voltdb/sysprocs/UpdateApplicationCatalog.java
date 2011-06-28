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

package org.voltdb.sysprocs;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.CommandLog;
import org.voltdb.catalog.Procedure;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;

@ProcInfo(singlePartition = false)
public class UpdateApplicationCatalog extends VoltSystemProcedure {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster)
    {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
    }

    @Override
    public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("UpdateApplicationCatalog was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }


    void validateCRC(String catalogURL, long crc)
    {
        long localcrc;
        try {
            localcrc = new InMemoryJarfile(catalogURL).getCRC();
        } catch (IOException e) {
            throw new VoltAbortException("Error reading Catalog URL.");
        }

        if (localcrc != crc)
            throw new VoltAbortException("Error reading Catalog URL.");
    }

    /**
     * Parameters to run are provided internally and do not map to the
     * user's input.
     * @param ctx
     * @param catalogDiffCommands
     * @param catalogURL
     * @param expectedCatalogVersion
     * @return Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String catalogDiffCommands, String catalogURL,
            int expectedCatalogVersion, String deploymentURL,
            long deploymentCRC)
    {
        CatalogContext oldContext = VoltDB.instance().getCatalogContext();
        CommandLog commandLog = oldContext.cluster.getLogconfig().get("log");

        // TODO: compute CRC for catalog vs. a crc provided by the initiator.
        // validateCRC(catalogURL, initiatorsCRC);

        // update the global version. only one site per node will accomplish this.
        // others will see there is no work to do and gracefully continue.
        // then update data at the local site.
        String commands = Encoder.decodeBase64AndDecompress(catalogDiffCommands);
        CatalogContext context =
            VoltDB.instance().catalogUpdate(commands, catalogURL, expectedCatalogVersion, getTransactionId(), deploymentCRC);
        ctx.getExecutionSite().updateCatalog(commands, context);

        /*
         * Take a local snapshot to truncate the command log. This is done on
         * each host, no need to coordinate. The sysproc already provides
         * sufficient coordination. This will be reverted once PRO-393 is done.
         */
        if (commandLog != null && commandLog.getEnabled()) {
            // This doesn't check if there's any ongoing cl truncation snapshots
            ZooKeeper zk = VoltDB.instance().getZK();
            try {
                zk.create("/request_truncation_snapshot", null, Ids.OPEN_ACL_UNSAFE,
                          CreateMode.PERSISTENT);
            } catch (NodeExistsException e) {

            } catch (Exception e) {
                hostLog.fatal("Requesting a truncation snapshot via ZK should always succeed", e);
                VoltDB.crashVoltDB();
            }
        }

        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        result.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {result});
    }
}
