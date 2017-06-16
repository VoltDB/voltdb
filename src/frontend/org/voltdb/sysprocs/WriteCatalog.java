package org.voltdb.sysprocs;

import java.util.concurrent.CompletableFuture;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;

import com.google_voltpatches.common.base.Throwables;

public class WriteCatalog extends UpdateApplicationBase {
    VoltLogger log = new VoltLogger("HOST");

    // Write the new catalog to a temporary jar file
    public CompletableFuture<ClientResponse> run(String catalogDiffCommands,
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
        assert(tablesThatMustBeEmpty != null);

        CatalogAndIds catalogStuff = null;
        try {
            catalogStuff = CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
        } catch (Exception e) {
            Throwables.propagate(e);
        }

        String replayInfo = "REPLAY ?";

        // update the global catalog if we get there first
        Pair<CatalogContext, CatalogSpecificPlanner> p =
        VoltDB.instance().catalogUpdate(
                catalogDiffCommands,
                catalogStuff.catalogBytes,
                catalogStuff.getCatalogHash(),
                expectedCatalogVersion,
                getID(),
                getID() + 1,
                catalogStuff.deploymentBytes,
                catalogStuff.getDeploymentHash(),
                requireCatalogDiffCmdsApplyToEE != 0,
                hasSchemaChange != 0,
                requiresNewExportGeneration != 0);

        return null;
    }
}
