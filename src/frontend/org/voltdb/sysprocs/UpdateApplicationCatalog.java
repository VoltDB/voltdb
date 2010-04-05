/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.*;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.JarReader;

@ProcInfo(singlePartition = false)
public class UpdateApplicationCatalog extends VoltSystemProcedure {

    final int ROUNDONE_DEP = 1 | DtxnConstants.MULTINODE_DEPENDENCY;
    final int ROUNDTWO_DEP = 2 | DtxnConstants.MULTIPARTITION_DEPENDENCY;

    @Override
        public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_catalogUpdateGlobal, this);
        site.registerPlanFragment(SysProcFragmentId.PF_catalogUptateExecSite, this);
    }

    @Override
    public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {

        VoltTable t = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
        Object[] paramObj = params.toArray();
        assert(paramObj.length > 0);
        String catalogDiffCommands = (String) paramObj[0];

        if (fragmentId == SysProcFragmentId.PF_catalogUpdateGlobal) {
            assert(paramObj.length == 4);
            String catalogURL = (String) paramObj[1];
            int expectedCatalogVersion = (Integer) paramObj[2];
            long crc = (Long) paramObj[3];
            if (updateVoltDBSingleton(context, catalogDiffCommands, catalogURL, expectedCatalogVersion, crc))
                t.addRow(1);
            else
                t.addRow(0);
            return new DependencyPair(ROUNDONE_DEP, t);
        }
        else if (fragmentId == SysProcFragmentId.PF_catalogUptateExecSite) {
            assert(paramObj.length == 1);
            if (updateExecutionSite(context, catalogDiffCommands))
                t.addRow(1);
            else
                t.addRow(0);
            return new DependencyPair(ROUNDTWO_DEP, t);
        }

        throw new RuntimeException("UpdateApplicationCatalog was given an invalid fragment id: " + String.valueOf(fragmentId));
    }

    boolean updateVoltDBSingleton(SystemProcedureExecutionContext context,
            String encodedCatalogDiffCommands, String catalogURL, int expectedCatalogVersion, long crc) {

        String catalogDiffCommands = Encoder.decodeBase64AndDecompress(encodedCatalogDiffCommands);

        // computer CRC for catalog
        long localcrc;
        try {
            localcrc = JarReader.crcForJar(catalogURL);
        } catch (IOException e) {
            throw new VoltAbortException("Error reading Catalog URL.");
        }

        if (localcrc != crc)
            throw new VoltAbortException("Error reading Catalog URL.");

        VoltDB.instance().catalogUpdate(catalogDiffCommands, catalogURL, expectedCatalogVersion);

        return true;
    }

    boolean updateExecutionSite(SystemProcedureExecutionContext context,
            String encodedCatalogDiffCommands) {

        String catalogDiffCommands = Encoder.decodeBase64AndDecompress(encodedCatalogDiffCommands);

        context.getExecutionSite().updateCatalog(catalogDiffCommands);

        return true;
    }

    public VoltTable[] run(String catalogDiffCommands, String catalogURL, int expectedCatalogVersion) {
        // computer CRC for catalog
        long crc;
        try {
            crc = JarReader.crcForJar(catalogURL);
        } catch (IOException e) {
            throw new VoltAbortException("Error reading Catalog URL.");
        }

        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];

        // Give the new catalog to all the nodes and have their VoltDB singleton update
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_catalogUpdateGlobal;
        pfs[0].outputDepId = ROUNDONE_DEP;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = true;
        pfs[0].parameters = new ParameterSet();
        pfs[0].parameters.setParameters(new Object[] { catalogDiffCommands, catalogURL, expectedCatalogVersion, crc });

        VoltTable[] retval = executeSysProcPlanFragments(pfs, ROUNDONE_DEP);
        assert(retval != null);
        assert(retval.length > 0);
        for (VoltTable t : retval)
            assert(t.asScalarLong() == 1);

        // Get all of the exec sites to update
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_catalogUptateExecSite;
        pfs[0].outputDepId = ROUNDTWO_DEP;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = true;
        pfs[0].nonExecSites = false;
        pfs[0].parameters = new ParameterSet();
        pfs[0].parameters.setParameters(new Object[] { catalogDiffCommands });

        retval = executeSysProcPlanFragments(pfs, ROUNDTWO_DEP);
        assert(retval != null);
        assert(retval.length > 0);
        for (VoltTable t : retval)
            assert(t.asScalarLong() == 1);

        return null;
    }
}
