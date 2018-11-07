/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.hsqldb_voltpatches.HSQLInterface;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Constants;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.ClassMatcher;
import org.voltdb.compiler.ClassMatcher.ClassNameMatchStatus;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.UniqueIdGenerator;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.base.Stopwatch;

/**
 * Base class for non-transactional sysprocs UpdateApplicationCatalog, UpdateClasses and Promote.
 * *ALSO* the base class for AdHocNTBase, which is the base class for AdHoc, AdHocSPForTest
 * and SwapTables.
 *
 * Has the common code for figuring out what changes need to be passed onto the transactional
 * UpdateCore procedure.
 *
 */
public abstract class UpdateApplicationBase extends VoltNTSystemProcedure {
    protected static final VoltLogger compilerLog = new VoltLogger("COMPILER");
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    private static final AtomicLong m_generationId = new AtomicLong(0);

    /**
     *
     * @param operationBytes The bytes for the catalog operation, if any. May be null in all cases.
     * For UpdateApplicationCatalog, this will contain the compiled catalog jarfile bytes
     * For UpdateClasses, this will contain the class jarfile bytes
     * For AdHoc DDL work, this will be null
     * @param operationString The string for the catalog operation, if any. May be null in all cases.
     * For UpdateApplicationCatalog, this will contain the deployment string to apply
     * For UpdateClasses, this will contain the class deletion patterns
     * For AdHoc DDL work, this will be null
     */
    public static CatalogChangeResult prepareApplicationCatalogDiff(String invocationName,
                                                                    final byte[] operationBytes,
                                                                    final String operationString,
                                                                    final String[] adhocDDLStmts,
                                                                    final byte[] replayHashOverride,
                                                                    final boolean isPromotion,
                                                                    final boolean useAdhocDDL,
                                                                    String hostname,
                                                                    String user)
    {
        final DrRoleType drRole = DrRoleType.fromValue(VoltDB.instance().getCatalogContext().getCluster().getDrrole());

        // create the change result and set up all the boiler plate
        CatalogChangeResult retval = new CatalogChangeResult();
        retval.tablesThatMustBeEmpty = new String[0]; // ensure non-null
        retval.hasSchemaChange = true;
        if (replayHashOverride != null) {
            retval.isForReplay = true;
        }

        try {
            // catalog change specific boiler plate
            CatalogContext context = VoltDB.instance().getCatalogContext();
            // Start by assuming we're doing an @UpdateApplicationCatalog.  If-ladder below
            // will complete with newCatalogBytes actually containing the bytes of the
            // catalog to be applied, and deploymentString will contain an actual deployment string,
            // or null if it still needs to be filled in.
            InMemoryJarfile newCatalogJar = null;
            InMemoryJarfile oldJar = context.getCatalogJar().deepCopy();
            String deploymentString = operationString;
            if ("@UpdateApplicationCatalog".equals(invocationName)) {
                compilerLog.info("@UpdateApplicationCatalog is invoked, current catalog version: " + context.catalogVersion);
                // Grab the current catalog bytes if @UAC had a null catalog from deployment-only update
                if ((operationBytes == null) || (operationBytes.length == 0)) {
                    newCatalogJar = oldJar;
                }
                else {
                    newCatalogJar = CatalogUtil.loadInMemoryJarFile(operationBytes);
                }
                // If the deploymentString is null, we'll fill it in with current deployment later
                // Otherwise, deploymentString has the right contents, don't need to touch it
            }
            else if ("@UpdateClasses".equals(invocationName)) {
                compilerLog.info("@UpdateClasses is invoked, modifying catalog classes.");
                // provided operationString is really a String with class patterns to delete,
                // provided newCatalogJar is the jarfile with the new classes
                if (operationBytes != null) {
                    newCatalogJar = new InMemoryJarfile(operationBytes);
                }
                try {
                    // Create a new hsql session to update classes, because it may races with
                    // @LoadSinglepartitionTable in Site thread
                    InMemoryJarfile modifiedJar = modifyCatalogClasses(context.catalog, oldJar, operationString,
                            newCatalogJar, drRole == DrRoleType.XDCR, context.m_ptool.getHSQLInterface());
                    if (modifiedJar == null) {
                        newCatalogJar = oldJar;
                    } else {
                        newCatalogJar = modifiedJar;
                    }
                }
                catch (ClassNotFoundException e) {
                    retval.errorMsg = "Classes not found in @UpdateClasses jar: " + e.getMessage();
                    return retval;
                }
                // Real deploymentString should be the current deployment, just set it to null
                // here and let it get filled in correctly later.
                deploymentString = null;

                // mark it as non-schema change
                retval.hasSchemaChange = false;
            }
            else if ("@AdHoc".equals(invocationName)) {
                // work.adhocDDLStmts should be applied to the current catalog
                try {
                    newCatalogJar = addDDLToCatalog(context.catalog, oldJar, adhocDDLStmts, drRole == DrRoleType.XDCR);
                }
                catch (IOException | VoltCompilerException e) {
                    retval.errorMsg = e.getMessage();
                    return retval;
                }
                catch (Exception ex) {
                    retval.errorMsg = "Unexpected condition occurred applying DDL statements: " + ex.getMessage();
                    return retval;
                }
                // Real deploymentString should be the current deployment, just set it to null
                // here and let it get filled in correctly later.
                deploymentString = null;
            }
            else {
                // Shouldn't ever get here
                retval.errorMsg = invocationName + " is not supported";
                return retval;
            }

            if (newCatalogJar == null) {
                // Shouldn't ever get here
                retval.errorMsg = "Unexpected failure during compiling the new catalog";
                return retval;
            }

            // get the diff between catalogs
            // try to get the new catalog from the params
            Pair<InMemoryJarfile, String> loadResults = null;
            try {
                loadResults = CatalogUtil.loadAndUpgradeCatalogFromJar(newCatalogJar, drRole == DrRoleType.XDCR);
            }
            catch (IOException ioe) {
                // Preserve a nicer message from the jarfile loading rather than
                // falling through to the ZOMG message in the big catch
                retval.errorMsg = ioe.getMessage();
                return retval;
            }
            retval.catalogBytes = loadResults.getFirst().getFullJarBytes();
            if (!retval.isForReplay) {
                retval.catalogHash = loadResults.getFirst().getSha1Hash();
            } else {
                retval.catalogHash = replayHashOverride;
            }

            String newCatalogCommands = CatalogUtil.getSerializedCatalogStringFromJar(loadResults.getFirst());
            if (newCatalogCommands == null) {
                retval.errorMsg = "Unable to read from catalog bytes";
                return retval;
            }
            retval.upgradedFromVersion = loadResults.getSecond();

            Catalog newCatalog = new Catalog();
            newCatalog.execute(newCatalogCommands);

            // Retrieve the original deployment string, if necessary
            if (deploymentString == null) {
                // Go get the deployment string from the current catalog context
                byte[] deploymentBytes = context.getDeploymentBytes();
                if (deploymentBytes != null) {
                    deploymentString = new String(deploymentBytes, Constants.UTF8ENCODING);
                }
                if (deploymentBytes == null || deploymentString == null) {
                    retval.errorMsg = "No deployment file provided and unable to recover previous deployment settings.";
                    return retval;
                }
            }

            // recompile deployment and add to catalog
            // this is necessary, even for @UpdateClasses that does not change deployment
            // because the catalog bytes does not contain any deployments but only schema related contents
            // the command log reply needs it to generate a correct catalog diff
            DeploymentType dt  = CatalogUtil.parseDeploymentFromString(deploymentString);
            if (dt == null) {
                retval.errorMsg = "Unable to update deployment configuration: Error parsing deployment string";
                return retval;
            }
            // Since sitesPerHost is no longer part of catalog, check it here before diff engine
            if (context.getDeployment().getCluster().getSitesperhost() != dt.getCluster().getSitesperhost()) {
                retval.errorMsg = "Unable to update deployment configuration: sites per host cannot be changed";
                return retval;
            }
            if (isPromotion && drRole == DrRoleType.REPLICA) {
                assert dt.getDr().getRole() == DrRoleType.REPLICA;
                dt.getDr().setRole(DrRoleType.MASTER);
            }

            String result = CatalogUtil.compileDeployment(newCatalog, dt, false);
            if (result != null) {
                retval.errorMsg = "Unable to update deployment configuration: " + result;
                return retval;
            }

            //In non legacy mode discard the path element.
            if (!VoltDB.instance().isRunningWithOldVerbs()) {
                dt.setPaths(null);
            }

            //Always get deployment after its adjusted.
            String newDeploymentString = CatalogUtil.getDeployment(dt, true);
            retval.deploymentBytes = newDeploymentString.getBytes(Constants.UTF8ENCODING);

            // make deployment hash from string
            retval.deploymentHash = CatalogUtil.makeDeploymentHash(retval.deploymentBytes);

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

            String commands = diff.commands();
            compilerLog.info(diff.getDescriptionOfChanges("@UpdateClasses".equals(invocationName)));

            retval.requireCatalogDiffCmdsApplyToEE = diff.requiresCatalogDiffCmdsApplyToEE();
            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = CompressionService.compressAndBase64Encode(commands);
            retval.diffCommandsLength = commands.length();
            String emptyTablesAndReasons[][] = diff.tablesThatMustBeEmpty();
            assert(emptyTablesAndReasons.length == 2);
            assert(emptyTablesAndReasons[0].length == emptyTablesAndReasons[1].length);
            retval.tablesThatMustBeEmpty = emptyTablesAndReasons[0];
            retval.reasonsForEmptyTables = emptyTablesAndReasons[1];
            retval.requiresSnapshotIsolation = diff.requiresSnapshotIsolation();
            retval.requiresNewExportGeneration = diff.requiresNewExportGeneration();
            retval.worksWithElastic = diff.worksWithElastic();
            retval.hasSecurityUserChange = diff.hasSecurityUserChanges();
        }
        catch (Exception e) {
            retval.errorMsg = "Unexpected error in catalog update from " + invocationName + ": " + e.getClass() + ", " +
                    e.getMessage();
        }
        return retval;
    }

    /**
     * Append the supplied adhoc DDL to the current catalog's DDL and recompile the
     * jarfile
     * @throws VoltCompilerException
     */
    protected static InMemoryJarfile addDDLToCatalog(Catalog oldCatalog, InMemoryJarfile jarfile, String[] adhocDDLStmts, boolean isXDCR)
    throws IOException, VoltCompilerException
    {
        StringBuilder sb = new StringBuilder();
        compilerLog.info("Applying the following DDL to cluster:");
        for (String stmt : adhocDDLStmts) {
            compilerLog.info("\t" + stmt);
            sb.append(stmt);
            sb.append(";\n");
        }
        String newDDL = sb.toString();
        compilerLog.trace("Adhoc-modified DDL:\n" + newDDL);

        VoltCompiler compiler = new VoltCompiler(isXDCR);
        compiler.compileInMemoryJarfileWithNewDDL(jarfile, newDDL, oldCatalog);
        return jarfile;
    }

    /**
     * @return NUll if no classes changed, otherwise return the update jar file.
     *
     * @throws ClassNotFoundException
     * @throws VoltCompilerException
     * @throws IOException
     */
    private static InMemoryJarfile modifyCatalogClasses(Catalog catalog, InMemoryJarfile jarfile, String deletePatterns,
            InMemoryJarfile newJarfile, boolean isXDCR, HSQLInterface hsql)
                    throws IOException, ClassNotFoundException, VoltCompilerException
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
                // Ignore root level, non-class file names
                boolean isRootFile = Paths.get(filename).getNameCount() == 1;
                if (isRootFile && !filename.endsWith(".class")) {
                    continue;
                }
                foundClasses = true;
                jarfile.put(e.getKey(), e.getValue());
            }
        }
        if (!deletedClasses && !foundClasses) {
            return null;
        }

        compilerLog.info("Updating java classes available to stored procedures");
        VoltCompiler compiler = new VoltCompiler(isXDCR);
        try {
            compiler.compileInMemoryJarfileForUpdateClasses(jarfile, catalog, hsql);
        } catch (ClassNotFoundException | VoltCompilerException | IOException ex) {
            throw ex;
        }

        return jarfile;
    }

    /** Check if something should run based on admin/paused/internal status */
    static protected boolean allowPausedModeWork(boolean internalCall, boolean adminConnection) {
        return (VoltDB.instance().getMode() != OperationMode.PAUSED ||
                internalCall ||
                adminConnection);
    }

    /** Error generating shortcut method */
    static protected CompletableFuture<ClientResponse> makeQuickResponse(byte statusCode, String msg) {
        ClientResponseImpl cri = new ClientResponseImpl(statusCode, new VoltTable[0], msg);
        CompletableFuture<ClientResponse> f = new CompletableFuture<>();
        f.complete(cri);
        return f;
    }

    /**
     * Run the catalog jar NT procedure to check and write the catalog file.
     * Check the results map from every host and return error message if needed.
     * @return  A String describing the error messages. If all hosts return success, NULL is returned.
     */
    protected String verifyAndWriteCatalogJar(CatalogChangeResult ccr)
    {
        String procedureName = "@VerifyCatalogAndWriteJar";

        CompletableFuture<Map<Integer,ClientResponse>> cf =
                callNTProcedureOnAllHosts(procedureName, ccr.catalogBytes, ccr.encodedDiffCommands,
                        ccr.catalogHash, ccr.deploymentBytes);

        Map<Integer, ClientResponse> resultMapByHost = null;
        String err;

        long timeoutSeconds = VerifyCatalogAndWriteJar.TIMEOUT;
        hostLog.info("Max timeout setting for VerifyCatalogAndWriteJar is " + timeoutSeconds + " seconds");

        try {
            Stopwatch sw = Stopwatch.createStarted();
            long elapsed = 0;
            while ((elapsed = sw.elapsed(TimeUnit.SECONDS)) < (timeoutSeconds)) {
                resultMapByHost = cf.getNow(null);
                if (resultMapByHost != null) {
                    sw.stop();
                    break;
                }

                if (elapsed < 5) {
                    // do not log under 5 seconds and sleep for 100 milliseconds
                    Thread.sleep(100);
                    continue;
                }
                hostLog.info(elapsed + " seconds has elapsed but " + procedureName + " is still wait for remote response."
                        + "The max timeout value is " + timeoutSeconds + " seconds.");
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            }
        } catch (Exception e) {
            err = procedureName + " run everywhere call failed: " + e.getMessage();
            hostLog.info(err + ", " + com.google_voltpatches.common.base.Throwables.getStackTraceAsString(e));
            return err;
        }

        if (resultMapByHost == null) {
            err = "An invocation of procedure " + procedureName + " on all hosts timed out.";
            hostLog.info(err);
            return err;
        }

        for (Entry<Integer, ClientResponse> entry : resultMapByHost.entrySet()) {
            if (entry.getValue().getStatus() != ClientResponseImpl.SUCCESS) {
                err = "The response from host " + entry.getKey().toString() +
                      " for " + procedureName + " returned failures: " + entry.getValue().getStatusString();
                compilerLog.info(err);

                // hide the internal NT-procedure @VerifyCatalogAndWriteJar from the client message
                return err;
            }
        }

        return null;
    }

    /**
     * Get a unique id for the next generation for export.
     * @return next generation id (a unique long value)
     */
    public static long getNextGenerationId() {
        return UniqueIdGenerator.makeIdFromComponents(System.currentTimeMillis(),
                                                      m_generationId.incrementAndGet(),
                                                      MpInitiator.MP_INIT_PID);
    }

    protected CompletableFuture<ClientResponse> updateApplication(String invocationName,
                                                                  final byte[] operationBytes,
                                                                  final String operationString,
                                                                  final String[] adhocDDLStmts,
                                                                  final byte[] replayHashOverride,
                                                                  final boolean isPromotion,
                                                                  final boolean useAdhocDDL)
    {
        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        CatalogChangeResult ccr = null;

        String errMsg = VoltZK.createActionBlocker(zk, VoltZK.catalogUpdateInProgress, CreateMode.EPHEMERAL,
                                                    hostLog, "catalog update(" + invocationName + ")" );
        if (errMsg != null) {
            return makeQuickResponse(ClientResponse.USER_ABORT, errMsg);
        }

        // Now we holds the UAC blocker lock
        try {
            ccr = prepareApplicationCatalogDiff(invocationName,
                                                operationBytes,
                                                operationString,
                                                adhocDDLStmts,
                                                replayHashOverride,
                                                isPromotion,
                                                useAdhocDDL,
                                                getHostname(),
                                                getUsername());
        } catch (Exception e) {
            VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
            errMsg = "Unexpected error during preparing catalog diffs: " + e.getMessage();
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, errMsg);
        }

        if (ccr.errorMsg != null) {
            VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, ccr.errorMsg);
        }
        // Log something useful about catalog upgrades when they occur.
        if (ccr.upgradedFromVersion != null) {
            compilerLog.info(String.format("catalog was automatically upgraded from version %s.",
                    ccr.upgradedFromVersion));
        }
        if (ccr.encodedDiffCommands.trim().length() == 0) {
            VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
            String msg = invocationName + " with no catalog changes was skipped.";
            compilerLog.info(msg);
            return makeQuickResponse(ClientResponseImpl.SUCCESS, msg);
        }
        if (isRestoring() && !isPromotion && "UpdateApplicationCatalog".equals(invocationName)) {
            // This means no more @UAC calls when using DDL mode.
            noteRestoreCompleted();
            compilerLog.info("No more @UpdateApplicationCatalog calls when using DDL mode");
        }

        // write the new catalog to a temporary jar file
        errMsg = verifyAndWriteCatalogJar(ccr);
        if (errMsg != null) {
            VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
            return makeQuickResponse(ClientResponseImpl.GRACEFUL_FAILURE, errMsg);
        }

        // only copy the current catalog when @UpdateCore could fail
        if (ccr.tablesThatMustBeEmpty.length != 0) {
            try {
                // read the current catalog bytes
                byte[] data = zk.getData(VoltZK.catalogbytes, false, null);
                // write to the previous catalog bytes place holder
                zk.setData(VoltZK.catalogbytesPrevious, data, -1);
            } catch (KeeperException | InterruptedException e) {
                VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
                errMsg = "error copying catalog bytes or write catalog bytes on ZK";
                return makeQuickResponse(ClientResponseImpl.GRACEFUL_FAILURE, errMsg);
            }
        }

        long genId = getNextGenerationId();
        // update the catalog jar
        CompletableFuture<ClientResponse> first = callProcedure("@UpdateCore",
                                                    ccr.encodedDiffCommands,
                                                    ccr.expectedCatalogVersion,
                                                    genId,
                                                    ccr.catalogBytes,
                                                    ccr.catalogHash,
                                                    ccr.deploymentBytes,
                                                    ccr.deploymentHash,
                                                    ccr.worksWithElastic ? 1 : 0,
                                                    ccr.tablesThatMustBeEmpty,
                                                    ccr.reasonsForEmptyTables,
                                                    ccr.requiresSnapshotIsolation ? 1 : 0,
                                                    ccr.requireCatalogDiffCmdsApplyToEE ? 1 : 0,
                                                    ccr.hasSchemaChange ?  1 : 0,
                                                    ccr.requiresNewExportGeneration ? 1 : 0,
                                                    ccr.hasSecurityUserChange ? 1 : 0);
        // inject unlocking callback
        CompletableFuture<ClientResponse> second = first.thenCompose(f -> CompletableFuture.supplyAsync(()-> {
            // holds the lock until now to guarantee sequential execution
            VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
            return f;
        }))
        .exceptionally(e -> {
            VoltZK.removeActionBlocker(zk, VoltZK.catalogUpdateInProgress, hostLog);
            return null;
        });
        return second;
    }

    protected void logCatalogUpdateInvocation(String procName) {
        if (getProcedureRunner().isUserAuthEnabled()) {
            String warnMsg = "A user from " + getProcedureRunner().getConnectionIPAndPort() +
                             " issued a " + procName + " to update the catalog.";
            hostLog.info(warnMsg);
        }
    }

}
