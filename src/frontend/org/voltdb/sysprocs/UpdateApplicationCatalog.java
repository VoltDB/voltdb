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

package org.voltdb.sysprocs;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;

import com.google.common.base.Throwables;

@ProcInfo(singlePartition = false)
public class UpdateApplicationCatalog extends VoltSystemProcedure {

    private static final int DEP_updateCatalog = (int)
            SysProcFragmentId.PF_updateCatalog | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_updateCatalogAggregate = (int)
            SysProcFragmentId.PF_updateCatalogAggregate;

    @Override
    public void init() {
        registerPlanFragment(SysProcFragmentId.PF_updateCatalog);
        registerPlanFragment(SysProcFragmentId.PF_updateCatalogAggregate);
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        if (fragmentId == SysProcFragmentId.PF_updateCatalog) {
            String catalogDiffCommands = (String)params.toArray()[0];
            String commands = Encoder.decodeBase64AndDecompress(catalogDiffCommands);
            int expectedCatalogVersion = (Integer)params.toArray()[1];
            long deploymentCRC = (Long)params.toArray()[2];

            byte catalogBytes[] = null;
            try {
                CatalogAndIds catalogStuff = CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
                catalogBytes = catalogStuff.bytes;
            } catch (Exception e) {
                Throwables.propagate(e);
            }

            Pair<CatalogContext, CatalogSpecificPlanner> p =
                    VoltDB.instance().catalogUpdate(
                            commands,
                            catalogBytes,
                            expectedCatalogVersion,
                            getVoltPrivateRealTransactionIdDontUseMe(),
                            getUniqueId(),
                            deploymentCRC);
            context.updateCatalog(commands, p.getFirst(), p.getSecond());


            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair(DEP_updateCatalog, result);
        } else if (fragmentId == SysProcFragmentId.PF_updateCatalogAggregate) {
            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            List<VoltTable> deps = dependencies.get(DEP_updateCatalog);
            for (VoltTable dep : deps) {
                while (dep.advanceRow())
                {
                    // this will add the active row of table
                    result.add(dep);
                }
            }
            return new DependencyPair(DEP_updateCatalogAggregate, result);
        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in UpdateApplicationCatalog",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code");
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

    private final VoltTable[] performCatalogUpdateWork(
            String catalogDiffCommands,
            int expectedCatalogVersion,
            long deploymentCRC)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateCatalog;
        pfs[0].outputDepId = DEP_updateCatalog;
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(catalogDiffCommands, expectedCatalogVersion, deploymentCRC);
        pfs[0].parameters = params;

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_updateCatalogAggregate;
        pfs[1].outputDepId = DEP_updateCatalogAggregate;
        pfs[1].inputDepIds  = new int[] { DEP_updateCatalog };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();


        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_updateCatalogAggregate);
        return results;
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
            String catalogDiffCommands, byte[] catalogBytes,
            int expectedCatalogVersion, String deploymentString,
            long deploymentCRC) throws Exception
    {
        // TODO: compute CRC for catalog vs. a crc provided by the initiator.
        // validateCRC(catalogURL, initiatorsCRC);

        // update the global version. only one site per node will accomplish this.
        // others will see there is no work to do and gracefully continue.
        // then update data at the local site.
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        CatalogUtil.setCatalogToZK(
                zk,
                expectedCatalogVersion + 1,
                getVoltPrivateRealTransactionIdDontUseMe(),
                getUniqueId(),
                catalogBytes);
        zk.setData(VoltZK.deploymentBytes, deploymentString.getBytes("UTF-8"), -1, new ZKUtil.StatCallback(), null);

        performCatalogUpdateWork(
                catalogDiffCommands,
                expectedCatalogVersion,
                deploymentCRC);

        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        result.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {result});
    }
}
