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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.licensing.Licensing;
import org.voltdb.utils.CatalogUtil;

/*
 * Some simple file system path checks, the goal is using NT procedure to do the check on
 * every node before doing the online upgrade.
 */
public class CheckUpgradePlanNT extends VoltNTSystemProcedure {
    private final static String SUCCESS = "Success";
    private final static int MINIMUM_MAJOR_VERSION = 7;
    private final static int MINIMUM_MINOR_VERSION = 2;

    public static class PrerequisitesCheckNT extends VoltNTSystemProcedure {

        public VoltTable run(String newKitPath, String newRootPath) throws InterruptedException, ExecutionException {
            String ret = checkVoltDBKitExistence(newKitPath);
            String ret2 = checkVoltDBRootExistence(newRootPath);
            String ret3 = validateXDCRRequirement();
            String warning = checkWarnings();
            VoltTable vt = new VoltTable(
                    new ColumnInfo[] { new ColumnInfo("KIT_CHECK_RESULT", VoltType.STRING),
                                       new ColumnInfo("ROOT_CHECK_RESULT", VoltType.STRING),
                                       new ColumnInfo("XDCR_CHECK_RESULT", VoltType.STRING),
                                       new ColumnInfo("WARNINGS", VoltType.STRING)});
            vt.addRow(ret, ret2, ret3, warning);
            return vt;
        }

        private static String checkVoltDBKitExistence(String newKitPath) {
            Path newKit = Paths.get(newKitPath);
            if (!Files.exists(newKit)) {
                return newKitPath + " doesn't exist.";
            } else if (!Files.isDirectory(newKit)) {
                return newKitPath + " is not a directory.";
            }
            // Check the new VoltDB kit version
            int[] newKitVersion = new int[2];
            try {
                String version = new String(Files.readAllBytes(Paths.get(newKitPath, "version.txt")));
                String cause = checkVersionString(version, newKitPath, newKitVersion);
                if (cause != null) {
                    return cause;
                }
            } catch (IOException | NumberFormatException e) {
                return "Failed to parse version string in the new VoltDB kit";
            }

            // Check version of current VoltDB instance
            int[] currentVersion = new int[2];
            try {
                String cause = checkVersionString(VoltDB.instance().getVersionString(), null, currentVersion);
                if (cause != null) {
                    return cause;
                }
            } catch (NumberFormatException e) {
                return "Failed to parse version of target VoltDB cluster";
            }

            // Check whether upgrade/downgrade across two major versions
            if (Math.abs(newKitVersion[0] - currentVersion[0]) >= 2) {
                return String.format("Online upgrade/downgrade across two major versions (%d.%d -> %d.%d) is not supported.",
                        currentVersion[0], currentVersion[1], newKitVersion[0], newKitVersion[1]);
            }

            return SUCCESS;
        }

        private static String checkVoltDBRootExistence(String newRootPath) {
            Path newRoot = Paths.get(newRootPath);
            if (!Files.exists(newRoot)) {
                return newRoot + " doesn't exist.";
            } else if (!Files.isDirectory(newRoot)) {
                return newRoot + " is not a directory.";
            }
            return SUCCESS;
        }

        private static String validateXDCRRequirement() {
            Licensing lic = VoltDB.instance().getLicensing();
            if (!lic.isFeatureAllowed("XDCR")) {
                return "Target VoltDB cluster doesn't have a valid XDCR license.";
            }

            CatalogContext context = VoltDB.instance().getCatalogContext();
            if (context.getDeployment().getDr() == null || context.getDeployment().getDr().getRole() != DrRoleType.XDCR) {
                return "Target VoltDB cluster must have XDCR enabled (set role=\"xdcr\" under DR tag of the deployment file).";
            }

            return SUCCESS;
        }

        /*
         * Create warnings if
         * 1) not all the user tables are DR table, or
         * 2) cluster runs on XDCR mode but is not listening on the DR port.
         */
        private static String checkWarnings() {
            StringBuilder warning = new StringBuilder();
            CatalogContext context = VoltDB.instance().getCatalogContext();
            if (context.getDeployment().getDr() != null && context.getDeployment().getDr().isListen() == false) {
                warning.append("Target VoltDB cluster is not listening on DR port.(set listen=\"true\" under DR tag of the deployment file)\n");
            }

            for (Table tb : context.database.getTables()) {
                if (!tb.getTypeName().equals(CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE)         /* skip conflict export table */
                        && !tb.getTypeName().equals(CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE)
                        && tb.getMaterializer() == null                                                 /* skip view table */
                        && !tb.getIsdred()) {
                    warning.append(tb.getTypeName()).append(" is not a DR table.").append("\n");
                }
            }

            if (warning.length() != 0) {
                return warning.substring(0, warning.length() - 1); // get rid of the last '\n'
            }
            return null;
        }

        private static String checkVersionString(String ver, String newKitPath, int[] versionNumber) throws NumberFormatException {
            String[] versions = ver.split("\\.");
            if (newKitPath != null && versionNumber.length < 2) {
                return "Illegal version string format found in " + newKitPath + ": " + ver;
            }
            int majorVersion = Integer.parseInt(versions[0].trim());
            int minorVersion = Integer.parseInt(versions[1].trim());
            if ( majorVersion < MINIMUM_MAJOR_VERSION ||
                            (majorVersion == MINIMUM_MAJOR_VERSION && minorVersion < MINIMUM_MINOR_VERSION)) {
                if (newKitPath != null) {
                    return "Version of new VoltDB kit is lower than the minimum supported version (v7.2)";
                }
                return "Version of target VoltDB cluster is lower than the minimum supported version (v7.2)";
            }
            versionNumber[0] = majorVersion;
            versionNumber[1] = minorVersion;
            return null;
        }
    }

    // Be user-friendly, return reasons of all failed checks.
    private static String[] aggregatePerHostResults(VoltTable vtable) {
        String[] ret = new String[2];
        vtable.advanceRow();
        String kitCheckResult = vtable.getString("KIT_CHECK_RESULT");
        String rootCheckResult = vtable.getString("ROOT_CHECK_RESULT");
        String xdcrCheckResult = vtable.getString("XDCR_CHECK_RESULT");
        StringBuilder result = new StringBuilder();
        if (!kitCheckResult.equals(SUCCESS)) {
            result.append(kitCheckResult).append("\n");
        }
        if (!rootCheckResult.equals(SUCCESS)) {
            result.append(rootCheckResult).append("\n");
        }
        if (!xdcrCheckResult.equals(SUCCESS)) {
            result.append(xdcrCheckResult);
        }
        if (result.length() == 0) {
            result.append(SUCCESS);
        }

        ret[0] = result.toString();
        String warnings = vtable.getString("WARNINGS");
        if (warnings != null) {
            ret[1] = warnings;
        }

        return ret;
    }

    public VoltTable run(String newKitPath, String newRootPath) throws InterruptedException, ExecutionException {

        CompletableFuture<Map<Integer,ClientResponse>> pf = callNTProcedureOnAllHosts("@PrerequisitesCheckNT", newKitPath, newRootPath);
        Map<Integer,ClientResponse> cr = pf.get();
        VoltTable vt = new VoltTable(
                new ColumnInfo[] { new ColumnInfo("HOST_ID", VoltType.INTEGER),
                                   new ColumnInfo("CHECK_RESULT", VoltType.STRING),
                                   new ColumnInfo("WARNINGS", VoltType.STRING)});
        cr.entrySet().stream()
        .forEach(e -> {
            String[] ret = aggregatePerHostResults(e.getValue().getResults()[0]);
            vt.addRow(e.getKey(), ret[0], ret[1]);
        });

        return vt;
    }
}
