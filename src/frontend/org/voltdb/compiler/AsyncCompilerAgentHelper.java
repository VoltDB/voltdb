/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.compiler;

import java.io.IOException;

import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class AsyncCompilerAgentHelper {
    public AsyncCompilerResult prepareApplicationCatalogDiff(CatalogChangeWork work) {
        // create the change result and set up all the boiler plate
        CatalogChangeResult retval = new CatalogChangeResult();
        retval.clientData = work.clientData;
        retval.clientHandle = work.clientHandle;
        retval.connectionId = work.connectionId;
        retval.adminConnection = work.adminConnection;
        retval.hostname = work.hostname;
        retval.invocationType = work.invocationType;
        retval.originalTxnId = work.originalTxnId;
        retval.originalUniqueId = work.originalUniqueId;

        // catalog change specific boiler plate
        CatalogContext context = VoltDB.instance().getCatalogContext();
        byte[] newCatalogBytes = work.catalogBytes;
        // Grab the current catalog bytes if the user didn't provide a catalog
        // (deployment-only change)
        if (work.catalogBytes == null) {
            try {
                newCatalogBytes = context.getCatalogJarBytes();
            }
            catch (IOException ioe) {
                retval.errorMsg = "Unexpected exception retrieving internal catalog bytes: " +
                    ioe.getMessage();
                return retval;
            }
        }
        retval.catalogBytes = newCatalogBytes;
        retval.catalogHash = CatalogUtil.makeCatalogOrDeploymentHash(newCatalogBytes);

        // UpdateApplicationCatalog uses the original null value to decide whether or not
        // to update the global ZK version of the deployment bytes, so just push that
        // along for now.
        retval.deploymentString = work.deploymentString;

        // get the diff between catalogs
        try {
            // try to get the new catalog from the params
            Pair<String, String> loadResults = CatalogUtil.loadAndUpgradeCatalogFromJar(newCatalogBytes, null);
            String newCatalogCommands = loadResults.getFirst();
            retval.upgradedFromVersion = loadResults.getSecond();
            if (newCatalogCommands == null) {
                retval.errorMsg = "Unable to read from catalog bytes";
                return retval;
            }
            Catalog newCatalog = new Catalog();
            newCatalog.execute(newCatalogCommands);

            String deploymentString = work.deploymentString;
            // work.deploymentString could be null if it wasn't provided to UpdateApplicationCatalog
            if (deploymentString == null) {
                // Go get the deployment string from ZK.  Hope it's there and up-to-date.  Yeehaw!
                byte[] deploymentBytes =
                    VoltDB.instance().getHostMessenger().getZK().getData(VoltZK.deploymentBytes, false, null);
                if (deploymentBytes != null) {
                    deploymentString = new String(deploymentBytes, "UTF-8");
                }
                if (deploymentBytes == null || deploymentString == null) {
                    retval.errorMsg = "No deployment file provided and unable to recover previous " +
                        "deployment settings.";
                    return retval;
                }
            }

            long result = CatalogUtil.compileDeploymentString(newCatalog, deploymentString, false, false);
            if (result < 0) {
                retval.errorMsg = "Unable to read from deployment file string";
                return retval;
            }

            retval.deploymentHash =
                CatalogUtil.makeCatalogOrDeploymentHash(deploymentString.getBytes("UTF-8"));

            // store the version of the catalog the diffs were created against.
            // verified when / if the update procedure runs in order to verify
            // catalogs only move forward
            retval.expectedCatalogVersion = context.catalogVersion;

            // compute the diff in StringBuilder
            CatalogDiffEngine diff = new CatalogDiffEngine(context.catalog, newCatalog);
            if (!diff.supported()) {
                retval.errorMsg = "The requested catalog change(s) are not supported:\n" + diff.errors();
                return retval;
            }

            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = Encoder.compressAndBase64Encode(diff.commands());
            retval.requiresSnapshotIsolation = diff.requiresSnapshotIsolation();
            retval.worksWithElastic = diff.worksWithElastic();
        }
        catch (Exception e) {
            e.printStackTrace();
            retval.encodedDiffCommands = null;
            retval.errorMsg = e.getMessage();
        }

        return retval;
    }
}
