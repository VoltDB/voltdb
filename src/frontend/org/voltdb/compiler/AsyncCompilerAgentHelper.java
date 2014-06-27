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

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.common.Constants;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;

public class AsyncCompilerAgentHelper
{
    private static final VoltLogger compilerLog = new VoltLogger("COMPILER");

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
        // (deployment-only change or adhoc DDL)
        if (work.catalogBytes == null) {
            try {
                newCatalogBytes = context.getCatalogJarBytes();
            }
            catch (IOException ioe) {
                retval.errorMsg = "Unexpected exception retrieving internal catalog bytes: " +
                    ioe.getMessage();
                return retval;
            }
            if (work.adhocDDLStmts != null) {
                try {
                    newCatalogBytes = addDDLToCatalog(newCatalogBytes, work.adhocDDLStmts);
                }
                catch (IOException ioe) {
                    retval.errorMsg = ioe.getMessage();
                    return retval;
                }
                if (newCatalogBytes == null) {
                    // Shouldn't ever get here
                    retval.errorMsg =
                        "Unexpected failure in applying DDL statements to original catalog";
                    return retval;
                }
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
            Pair<InMemoryJarfile, String> loadResults =
                CatalogUtil.loadAndUpgradeCatalogFromJar(newCatalogBytes);
            String newCatalogCommands =
                CatalogUtil.getSerializedCatalogStringFromJar(loadResults.getFirst());
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

    /**
     * Append the supplied adhoc DDL to the current catalog's DDL and recompile the
     * jarfile
     */
    private byte[] addDDLToCatalog(byte[] oldCatalogBytes, String[] adhocDDLStmts)
    throws IOException
    {
        VoltCompilerReader ddlReader = null;
        try {
            InMemoryJarfile jarfile = CatalogUtil.loadInMemoryJarFile(oldCatalogBytes);
            // Yoink the current cluster catalog's canonical DDL and append the supplied
            // adhoc DDL to it.
            String oldDDL = new String(jarfile.get(VoltCompiler.AUTOGEN_DDL_FILE_NAME),
                    Constants.UTF8ENCODING);
            StringBuilder sb = new StringBuilder();
            sb.append(oldDDL);
            sb.append("\n");
            for (String stmt : adhocDDLStmts) {
                sb.append(stmt);
                sb.append(";\n");
            }
            compilerLog.debug("Adhoc-modified DDL:\n" + sb.toString());
            // Put the new DDL back into the InMemoryJarfile we built because that's
            // the artifact the compiler is expecting to work on.  This allows us to preserve any
            // stored procedure classes and the class loader that came with the catalog
            // before we appended to it.
            ddlReader =
                new VoltCompilerStringReader(VoltCompiler.AUTOGEN_DDL_FILE_NAME, sb.toString());
            ddlReader.putInJar(jarfile, VoltCompiler.AUTOGEN_DDL_FILE_NAME);
            VoltCompiler compiler = new VoltCompiler();
            compiler.compileInMemoryJarfile(jarfile);
            return jarfile.getFullJarBytes();
        }
        finally {
            if (ddlReader != null) {
                try {
                    ddlReader.close();
                }
                catch (IOException ioe) {}
            }
        }
    }
}
