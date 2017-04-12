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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

/*
 * Some simple file system path checks, the goal is using NT proc to do the check on
 * every node before doing the in-service upgrade.
 */
public class CheckUpgradePlanNT extends VoltNTSystemProcedure {

    public static class PrerequisitesCheckNT extends VoltNTSystemProcedure {
        private final static int MINIMUM_MAJOR_VERSION = 7;
        private final static int MINIMUM_MINOR_VERSION = 2;

        public VoltTable run(String newKitPath, String newRootPath) throws InterruptedException, ExecutionException {

            System.out.println("Calling @CheckEveryWhere...");
            String ret = checkVoltDBKitExistence(newKitPath);
            String ret2 = checkVoltDBRootExistence(newRootPath);
            VoltTable vt = new VoltTable(
                    new ColumnInfo[] { new ColumnInfo("KIT_CHECK_RESULT", VoltType.STRING),
                                       new ColumnInfo("ROOT_CHECK_RESULT", VoltType.STRING) });
            vt.addRow(ret, ret2);
            return vt;
        }

        private static String checkVoltDBKitExistence(String newKitPath) {
            Path newKit = Paths.get(newKitPath);
            if (Files.exists(newKit) && Files.isDirectory(newKit)) {
                try {
                    String version = new String(Files.readAllBytes(Paths.get(newKitPath, "version.txt")));
                    String[] versionNumber = version.split(".");
                    if (versionNumber.length < 2) {
                        return "Illegal version string format found in " + newKitPath;
                    }
                    Integer majorVersion = Integer.getInteger(versionNumber[0]);
                    Integer minorVersion = Integer.getInteger(versionNumber[1]);
                    if (majorVersion != null && minorVersion != null &&
                            (majorVersion < MINIMUM_MAJOR_VERSION ||
                                    (majorVersion == MINIMUM_MAJOR_VERSION && minorVersion < MINIMUM_MINOR_VERSION))) {
                        return "Version of new VoltDB kit is lower than the minimum supported version (v" +
                                MINIMUM_MAJOR_VERSION + "." + MINIMUM_MINOR_VERSION + ")";
                    } else {
                        return "Success";
                    }
                } catch (IOException e) {
                    return "Failed to read version string in the new VoltDB kit";
                }
            } else {
                return newKitPath + " doesn't exist.";
            }
        }

        private static String checkVoltDBRootExistence(String newRootPath) {
            Path newRoot = Paths.get(newRootPath);
            if (!Files.exists(newRoot)) {
                try {
                    Files.createDirectories(newRoot);
                } catch (IOException e) {
                    return "Failed to create " + newRootPath + " for the new VoltDB root.";
                }
            } else if (!Files.isDirectory(newRoot)) {
                return newRoot + " does exist, but it doesn't look like a directory.";
            }
            return "Success";
        }
    }

    // Be user-friendly, return reasons of all the failed check.
    private static String aggregatePerHostResults(VoltTable vtable) {
        vtable.advanceRow();
        String kitCheckResult = vtable.getString("KIT_CHECK_RESULT");
        String rootCheckResult = vtable.getString("ROOT_CHECK_RESULT");
        StringBuilder result = new StringBuilder();
        if (!kitCheckResult.equals("Success")){
            result.append(kitCheckResult);
        }
        if (!rootCheckResult.equals("Success")) {
            result.append(rootCheckResult);
        }
        if (result.length() == 0) {
            result.append("Success");
        }

        return result.toString();
    }

    public VoltTable run(String newKitPath, String newRootPath) throws InterruptedException, ExecutionException {

        CompletableFuture<Map<Integer,ClientResponse>> pf = callNTProcedureOnAllHosts("@PrerequisitesCheckNT", newKitPath, newRootPath);
        Map<Integer,ClientResponse> cr = pf.get();
        VoltTable vt = new VoltTable(
                new ColumnInfo[] { new ColumnInfo("HOST_ID", VoltType.INTEGER),
                                   new ColumnInfo("CHECK_RESULT", VoltType.STRING) });
        cr.entrySet().stream()
        .forEach(e -> {
            vt.addRow(e.getKey(), aggregatePerHostResults(e.getValue().getResults()[0]));
        });

        return vt;
    }
}
