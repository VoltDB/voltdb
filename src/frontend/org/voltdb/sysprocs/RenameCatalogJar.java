/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.ParameterSet;
import org.voltdb.ReplicationRole;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltTableUtil;

import com.google_voltpatches.common.base.Throwables;

public class RenameCatalogJar extends VoltSystemProcedure {

    public static VoltLogger log = new VoltLogger("HOST");

    private static final int DEP_updateCatalog = (int)
            SysProcFragmentId.PF_updateCatalog | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_updateCatalogAggregate = (int)
            SysProcFragmentId.PF_updateCatalogAggregate;

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
                SysProcFragmentId.PF_updateCatalog,
                SysProcFragmentId.PF_updateCatalogAggregate};
    }

    @SuppressWarnings("deprecation")
    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_updateCatalog) {
            String catalogDiffCommands = (String)params.toArray()[0];
            String commands = Encoder.decodeBase64AndDecompress(catalogDiffCommands);
            int expectedCatalogVersion = (Integer)params.toArray()[1];
            boolean requiresSnapshotIsolation = ((Byte) params.toArray()[2]) != 0;
            boolean requireCatalogDiffCmdsApplyToEE = ((Byte) params.toArray()[3]) != 0;
            boolean hasSchemaChange = ((Byte) params.toArray()[4]) != 0;
            boolean requiresNewExportGeneration = ((Byte) params.toArray()[5]) != 0;

            CatalogAndIds catalogStuff = null;
            try {
                catalogStuff = CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
            } catch (Exception e) {
                Throwables.propagate(e);
            }

            String replayInfo = "replay ... ";

            // if this is a new catalog, do the work to update
            if (context.getCatalogVersion() == expectedCatalogVersion) {

                log.warn("==================RenameCatalogJar=====================");
                log.warn("context cat ver: " + context.getCatalogVersion());
                log.warn("zk cat ver: " + catalogStuff.version);
                log.warn("expected cat ver: " + expectedCatalogVersion);
                log.warn("========================================================");

                // Rename the catalog jar
                // This still works here, and only the first site will complete the
                // context update + renaming procedure
                Pair<CatalogContext, CatalogSpecificPlanner> p =
                VoltDB.instance().catalogUpdate(
                        commands,
                        catalogStuff.catalogBytes,
                        catalogStuff.getCatalogHash(),
                        expectedCatalogVersion,
                        DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this),
                        getUniqueId(),
                        catalogStuff.deploymentBytes,
                        catalogStuff.getDeploymentHash(),
                        requireCatalogDiffCmdsApplyToEE,
                        hasSchemaChange,
                        requiresNewExportGeneration);

                // If the cluster is in master role only (not replica or XDCR), reset trackers.
                // The producer would have been turned off by the code above already.
                if (VoltDB.instance().getReplicationRole() == ReplicationRole.NONE &&
                    !VoltDB.instance().getReplicationActive()) {
                    context.resetDrAppliedTracker();
                }

                // update the local catalog.  Safe to do this thanks to the check to get into here.
                long uniqueId = m_runner.getUniqueId();
                long spHandle = m_runner.getTxnState().getNotice().getSpHandle();
                context.updateCatalog(commands, p.getFirst(), p.getSecond(),
                        requiresSnapshotIsolation, uniqueId, spHandle,
                        requireCatalogDiffCmdsApplyToEE, requiresNewExportGeneration);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("Site %s completed catalog update with catalog hash %s, deployment hash %s%s.",
                            CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()),
                            Encoder.hexEncode(catalogStuff.getCatalogHash()).substring(0, 10),
                            Encoder.hexEncode(catalogStuff.getDeploymentHash()).substring(0, 10),
                            replayInfo));
                }
            }
            // if seen before by this code, then check to see if this is a restart
            else if (context.getCatalogVersion() == (expectedCatalogVersion + 1) &&
                    Arrays.equals(context.getCatalogHash(), catalogStuff.getCatalogHash()) &&
                    Arrays.equals(context.getDeploymentHash(), catalogStuff.getDeploymentHash())) {
                log.info(String.format("Site %s will NOT apply an assumed restarted and identical catalog update with catalog hash %s and deployment hash %s.",
                            CoreUtils.hsIdToString(m_site.getCorrespondingSiteId()),
                            Encoder.hexEncode(catalogStuff.getCatalogHash()),
                            Encoder.hexEncode(catalogStuff.getDeploymentHash())));
            }
            else {
                VoltDB.crashLocalVoltDB("Invalid catalog update.  Expected version: " + expectedCatalogVersion +
                        ", current version: " + context.getCatalogVersion(), false, null);
            }

            VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
            result.addRow(VoltSystemProcedure.STATUS_OK);
            return new DependencyPair.TableDependencyPair(DEP_updateCatalog, result);
        } else if ( fragmentId == SysProcFragmentId.PF_updateCatalogAggregate) {
            // Is this really doing work ?
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_updateCatalog));
            return new DependencyPair.TableDependencyPair(DEP_updateCatalogAggregate, result);
        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in RenameCatalogJar",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code here !!!");
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String catalogDiffCommands,
                           byte[] catalogHash,
                           byte[] catalogBytes,
                           int expectedCatalogVersion,
                           String deploymentString,
                           String[] tablesThatMustBeEmpty,
                           String[] reasonsForEmptyTables,
                           byte requiresSnapshotIsolation,
                           byte worksWithElastic,
                           byte[] deploymentHash,
                           byte requireCatalogDiffCmdsApplyToEE,
                           byte hasSchemaChange,
                           byte requiresNewExportGeneration)
    {
        performCatalogUpdateWork(
                catalogDiffCommands,
                expectedCatalogVersion,
                requiresSnapshotIsolation,
                requireCatalogDiffCmdsApplyToEE,
                hasSchemaChange,
                requiresNewExportGeneration);

        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        result.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {result});
    }

    private final VoltTable[] performCatalogUpdateWork(
            String catalogDiffCommands,
            int expectedCatalogVersion,
            byte requiresSnapshotIsolation,
            byte requireCatalogDiffCmdsApplyToEE,
            byte hasSchemaChange,
            byte requiresNewExportGeneration)
    {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // Now do the real work
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_updateCatalog;
        pfs[0].outputDepId = DEP_updateCatalog;
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(
                catalogDiffCommands,
                expectedCatalogVersion,
                requiresSnapshotIsolation,
                requireCatalogDiffCmdsApplyToEE,
                hasSchemaChange,
                requiresNewExportGeneration);

        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_updateCatalogAggregate;
        pfs[1].outputDepId = DEP_updateCatalogAggregate;
        pfs[1].inputDepIds  = new int[] { DEP_updateCatalog };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_updateCatalogAggregate);
        return results;
    }

}
