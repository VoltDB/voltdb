/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Map.Entry;

import org.hsqldb_voltpatches.Database;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.catalog.Procedure;
import org.voltdb.common.Constants;
import org.voltdb.compiler.ClassMatcher.ClassNameMatchStatus;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;

import com.sun.org.apache.xml.internal.resolver.Catalog;

public class AsyncCompilerAgentHelper
{
    private static final VoltLogger compilerLog = new VoltLogger("COMPILER");
    private final LicenseApi m_licenseApi;

    public AsyncCompilerAgentHelper(LicenseApi licenseApi) {
        m_licenseApi = licenseApi;
    }

    public CatalogChangeResult prepareApplicationCatalogDiff(CatalogChangeWork work) {
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
        retval.user = work.user;
        retval.tablesThatMustBeEmpty = new String[0]; // ensure non-null
        retval.hasSchemaChange = true;

        try {
            // catalog change specific boiler plate
            CatalogContext context = VoltDB.instance().getCatalogContext();
            // Start by assuming we're doing an @UpdateApplicationCatalog.  If-ladder below
            // will complete with newCatalogBytes actually containing the bytes of the
            // catalog to be applied, and deploymentString will contain an actual deployment string,
            // or null if it still needs to be filled in.
            InMemoryJarfile newCatalogJar = null;
            InMemoryJarfile oldJar = context.getCatalogJar().deepCopy();
            boolean updatedClass = false;

            String deploymentString = work.operationString;
            if ("@UpdateApplicationCatalog".equals(work.invocationName)) {
                // Grab the current catalog bytes if @UAC had a null catalog from deployment-only update
                if (work.operationBytes == null) {
                    newCatalogJar = oldJar;
                } else {
                    newCatalogJar = CatalogUtil.loadInMemoryJarFile(work.operationBytes);
                }
                // If the deploymentString is null, we'll fill it in with current deployment later
                // Otherwise, deploymentString has the right contents, don't need to touch it
            }
            else if ("@UpdateClasses".equals(work.invocationName)) {
                // provided operationString is really a String with class patterns to delete,
                // provided newCatalogJar is the jarfile with the new classes
                if (work.operationBytes != null) {
                    newCatalogJar = new InMemoryJarfile(work.operationBytes);
                }
                try {
                    newCatalogJar = modifyCatalogClasses(context.catalog, oldJar, work.operationString,
                            newCatalogJar);
                }
                catch (ClassNotFoundException e) {
                    retval.errorMsg = "Unexpected exception @UpdateClasses modifying classes " +
                        "from catalog: " + e.getMessage();
                    return retval;
                }
                // Real deploymentString should be the current deployment, just set it to null
                // here and let it get filled in correctly later.
                deploymentString = null;
                updatedClass = true;

                // mark it as non-schema change
                retval.hasSchemaChange = false;
            }
            else if ("@AdHoc".equals(work.invocationName)) {
                // work.adhocDDLStmts should be applied to the current catalog
                try {
                    newCatalogJar = addDDLToCatalog(context.catalog, oldJar,
                            work.adhocDDLStmts);
                }
                catch (VoltCompilerException vce) {
                    retval.errorMsg = vce.getMessage();
                    return retval;
                }
                catch (IOException ioe) {
                    retval.errorMsg = "Unexpected exception applying DDL statements to " +
                        "original catalog: " + ioe.getMessage();
                    return retval;
                }
                if (newCatalogJar == null) {
                    // Shouldn't ever get here
                    retval.errorMsg =
                        "Unexpected failure in applying DDL statements to original catalog";
                    return retval;
                }
                // Real deploymentString should be the current deployment, just set it to null
                // here and let it get filled in correctly later.
                deploymentString = null;
            }
            else {
                retval.errorMsg = "Unexpected work in the AsyncCompilerAgentHelper: " +
                    work.invocationName;
                return retval;
            }

            // get the diff between catalogs
            // try to get the new catalog from the params
            Pair<InMemoryJarfile, String> loadResults = null;
            try {
                loadResults = CatalogUtil.loadAndUpgradeCatalogFromJar(newCatalogJar);
            }
            catch (IOException ioe) {
                // Preserve a nicer message from the jarfile loading rather than
                // falling through to the ZOMG message in the big catch
                retval.errorMsg = ioe.getMessage();
                return retval;
            }
            retval.catalogBytes = loadResults.getFirst().getFullJarBytes();
            retval.catalogHash = loadResults.getFirst().getSha1Hash();
            String newCatalogCommands =
                CatalogUtil.getSerializedCatalogStringFromJar(loadResults.getFirst());
            retval.upgradedFromVersion = loadResults.getSecond();
            if (newCatalogCommands == null) {
                retval.errorMsg = "Unable to read from catalog bytes";
                return retval;
            }
            Catalog newCatalog = new Catalog();
            newCatalog.execute(newCatalogCommands);

            String result = CatalogUtil.checkLicenseConstraint(newCatalog, m_licenseApi);
            if (result != null) {
                retval.errorMsg = result;
                return retval;
            }

            // Retrieve the original deployment string, if necessary
            if (deploymentString == null) {
                // Go get the deployment string from the current catalog context
                byte[] deploymentBytes = context.getDeploymentBytes();
                if (deploymentBytes != null) {
                    deploymentString = new String(deploymentBytes, Constants.UTF8ENCODING);
                }
                if (deploymentBytes == null || deploymentString == null) {
                    retval.errorMsg = "No deployment file provided and unable to recover previous " +
                        "deployment settings.";
                    return retval;
                }
            }

            result =
                CatalogUtil.compileDeploymentString(newCatalog, deploymentString, false);
            if (result != null) {
                retval.errorMsg = "Unable to update deployment configuration: " + result;
                return retval;
            }

            retval.deploymentString = deploymentString;
            retval.deploymentHash =
                CatalogUtil.makeDeploymentHash(deploymentString.getBytes(Constants.UTF8ENCODING));

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
            compilerLog.info(diff.getDescriptionOfChanges(updatedClass));

            retval.requireCatalogDiffCmdsApplyToEE = diff.requiresCatalogDiffCmdsApplyToEE();
            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = Encoder.compressAndBase64Encode(diff.commands());
            retval.tablesThatMustBeEmpty = diff.tablesThatMustBeEmpty();
            retval.reasonsForEmptyTables = diff.reasonsWhyTablesMustBeEmpty();
            retval.requiresSnapshotIsolation = diff.requiresSnapshotIsolation();
            retval.worksWithElastic = diff.worksWithElastic();
        }
        catch (Exception e) {
            String msg = "Unexpected error in adhoc or catalog update: " + e.getClass() + ", " +
                e.getMessage();
            compilerLog.warn(msg, e);
            retval.encodedDiffCommands = null;
            retval.errorMsg = msg;
        }

        return retval;
    }

    /**
     * Append the supplied adhoc DDL to the current catalog's DDL and recompile the
     * jarfile
     * @throws VoltCompilerException
     */
    private InMemoryJarfile addDDLToCatalog(Catalog oldCatalog, InMemoryJarfile jarfile, String[] adhocDDLStmts)
    throws IOException, VoltCompilerException
    {
        VoltCompilerReader ddlReader = null;
        try {
            StringBuilder sb = new StringBuilder();
            compilerLog.info("Applying the following DDL to cluster:");
            for (String stmt : adhocDDLStmts) {
                compilerLog.info("\t" + stmt);
                sb.append(stmt);
                sb.append(";\n");
            }
            String newDDL = sb.toString();
            compilerLog.trace("Adhoc-modified DDL:\n" + newDDL);

            VoltCompiler compiler = new VoltCompiler();
            compiler.compileInMemoryJarfileWithNewDDL(jarfile, newDDL, oldCatalog);
            return jarfile;
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

    private InMemoryJarfile modifyCatalogClasses(Catalog catalog, InMemoryJarfile jarfile, String deletePatterns,
            InMemoryJarfile newJarfile) throws ClassNotFoundException
    {
        // modify the old jar in place based on the @UpdateClasses inputs, and then
        // recompile it if necessary
        boolean deletedClasses = false;
        if (deletePatterns != null) {
            String[] patterns = deletePatterns.split(",");
            ClassMatcher matcher = new ClassMatcher();
            // Need to concatenate all the classnames together for ClassMatcher
            String currentClasses = "";
            for (String classname : jarfile.getLoader().getClassNames()) {
                currentClasses = currentClasses.concat(classname + "\n");
            }
            matcher.m_classList = currentClasses;
            for (String pattern : patterns) {
                ClassNameMatchStatus status = matcher.addPattern(pattern.trim());
                if (status == ClassNameMatchStatus.MATCH_FOUND) {
                    deletedClasses = true;
                }
            }

            for (String classname : matcher.getMatchedClassList()) {
                jarfile.removeClassFromJar(classname);
            }
        }
        boolean foundClasses = false;
        if (newJarfile != null) {
            for (Entry<String, byte[]> e : newJarfile.entrySet()) {
                String filename = e.getKey();
                if (!filename.endsWith(".class")) {
                    continue;
                }
                foundClasses = true;
                jarfile.put(e.getKey(), e.getValue());
            }
        }
        if (deletedClasses || foundClasses) {
            compilerLog.info("Checking java classes available to stored procedures");
            // TODO: check the jar classes on all nodes
            Database db = VoltCompiler.getCatalogDatabase(catalog);
            for (Procedure proc: db.getProcedures()) {
                // single statement procedure does not need to check class loading
                if (proc.getHasjava() == false) continue;

                if (! VoltCompilerUtils.containsClassName(jarfile, proc.getClassname())) {
                    throw new ClassNotFoundException("Cannot load class for procedure " + proc.getClassname());
                }
            }
        }
        return jarfile;
    }
}
