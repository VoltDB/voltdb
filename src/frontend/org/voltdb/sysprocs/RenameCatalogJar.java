package org.voltdb.sysprocs;

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
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Throwables;

public class RenameCatalogJar extends VoltSystemProcedure {

    public static VoltLogger log = new VoltLogger("HOST");

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
                SysProcFragmentId.PF_updateCatalog,
                SysProcFragmentId.PF_updateCatalogAggregate};
    }

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

            log.warn("========================================================");
            log.warn("context cat ver: " + context.getCatalogVersion());
            log.warn("zk cat ver: " + catalogStuff.version);
            log.warn("expected cat ver: " + expectedCatalogVersion);
            log.warn("========================================================");

            // if this is a new catalog, do the work to update
            if (context.getCatalogVersion() == expectedCatalogVersion) {

                // update the global catalog if we get there first
                @SuppressWarnings("deprecation")
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
        } else {
            VoltDB.crashLocalVoltDB(
                    "Received unrecognized plan fragment id " + fragmentId + " in RenameCatalogJar",
                    false,
                    null);
        }
        throw new RuntimeException("Should not reach this code here !!!");
    }

    public VoltTable[] run() {
        return null;
    }

}
