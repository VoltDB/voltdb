/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.NTProcedureService;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndDeployment;
import org.voltdb.utils.CompressionService;

/**
 *
 * This system NT-procedure is used by UpdateCore related work to verify and write
 * the catalog bytes. The purpose of this procedure is to put the catalog verification and write
 * work on the asynchronous thread and reduce its blocking time.
 */
public class VerifyCatalogAndWriteJar extends UpdateApplicationBase {

    public final static HashMap<Integer, String> SupportedJavaVersionMap = new HashMap<>();
    static {
        SupportedJavaVersionMap.put(45, "Java 1.1");
        SupportedJavaVersionMap.put(46, "Java 1.2");
        SupportedJavaVersionMap.put(47, "Java 1.3");
        SupportedJavaVersionMap.put(48, "Java 1.4");
        SupportedJavaVersionMap.put(49, "Java 5");
        SupportedJavaVersionMap.put(50, "Java 6");
        SupportedJavaVersionMap.put(51, "Java 7");
        SupportedJavaVersionMap.put(52, "Java 8");
        SupportedJavaVersionMap.put(53, "Java 9");
        SupportedJavaVersionMap.put(54, "Java 10");
        SupportedJavaVersionMap.put(55, "Java 11");
    }

    private static VoltLogger log = new VoltLogger("HOST");

    private static long getTimeoutValue() {
        long timeoutSeconds = 600; // default value 600 seconds

        String timeoutEnvString = null;
        try {
            timeoutEnvString = System.getenv(NTProcedureService.NTPROCEDURE_RUN_EVERYWHERE_TIMEOUT);
        } catch (SecurityException ex) {
            log.warn("Trying to access system environment variable " + ex.getMessage() + " failed, "
                    + "use default value " + timeoutSeconds);
            return timeoutSeconds;
        }

        if (timeoutEnvString == null) {
            return timeoutSeconds;
        }

        try {
            timeoutSeconds = Long.parseLong(timeoutEnvString);
        } catch (NumberFormatException ex) {
            VoltDB.crashLocalVoltDB("Invalid system environment setting for "
                        + NTProcedureService.NTPROCEDURE_RUN_EVERYWHERE_TIMEOUT
                        + " in "+ timeoutEnvString + " seconds.");
        }
        if (timeoutSeconds < 10) {
            VoltDB.crashLocalVoltDB(" NT run-everywhere timeout value needs to be greater than 10 seconds, now the setting is " +
                    timeoutSeconds + " seconds.");
        }
        return timeoutSeconds;
    }

    public final static long TIMEOUT = getTimeoutValue();


    public CompletableFuture<ClientResponse> run(String encodedDiffCommands, int nextCatalogVersion)
    {
        // This should only be called once on each host
        String diffCommands = CompressionService.decodeBase64AndDecompress(encodedDiffCommands);
        log.info("Verify user procedure classes and write catalog jar (compressed size = " + encodedDiffCommands.length() +
                ", uncompressed size = " + diffCommands.length() +")");

        CatalogAndDeployment cad = null;
        try {
            cad = CatalogUtil.getStagingCatalogFromZK(VoltDB.instance().getHostMessenger().getZK(),
                    nextCatalogVersion);
        } catch (KeeperException | InterruptedException e) {
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE,
                    "unexpected error reading staging catalog from zookeeper: " + e);
        }

        String err = VoltDB.instance().verifyJarAndPrepareProcRunners(
                cad.catalogBytes, diffCommands, cad.catalogHash, cad.deploymentBytes);
        if (err != null) {
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE,
                    "unexpected error verifying classes or preparing procedure runners: " + err);
        }

        // Write the new catalog to a temporary jar file
        try {
            VoltDB.instance().writeCatalogJar(cad.catalogBytes);
        } catch (Exception e) {
            // Catalog disk write failed, include the message
            VoltDB.instance().cleanUpTempCatalogJar();
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE,
                    "unexpected error writting catalog jar: " + e.getMessage());
        }

        return makeQuickResponse(ClientResponseImpl.SUCCESS, "");
    }
}
