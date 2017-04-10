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
import java.util.concurrent.ExecutionException;

import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/*
 * Some simple file system path checks, the goal is using NT proc to do the check on
 * every node before doing the in-service upgrade.
 */
public class CheckUpgradePlan extends VoltNTSystemProcedure {

    private final static int MINIMUM_MAJOR_VERSION = 7;
    private final static int MINIMUM_MINOR_VERSION = 2;

    public VoltTable run(String newKitPath, String newRootPath) throws InterruptedException, ExecutionException {

        String ret = checkVoltDBKitExistence(newKitPath);

        String ret2 = checkVoltDBRootExistence(newRootPath);

        VoltTable vt = new VoltTable(
                new ColumnInfo[] { new ColumnInfo("CHECK_RESULT", VoltType.STRING) });
        vt.addRow(ret);
        vt.addRow(ret2);

        return vt;
    }

    private String checkVoltDBKitExistence(String newKitPath) {
        Path newKit = Paths.get(newKitPath);
        if (Files.exists(newKit) && Files.isDirectory(newKit)) {
            try {
                String version = new String(Files.readAllBytes(Paths.get(newKitPath, "version.txt")));
                String[] versionNumber = version.split(".");
                if (versionNumber.length < 2) {
                    return "Illegal version string format found in the new VoltDB kit.";
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
            return "The path you specify as the VoltDB kit doesn't exist, please check again before you run the command.";
        }
    }

    private String checkVoltDBRootExistence(String newRootPath) {
        Path newRoot = Paths.get(newRootPath);
        if (!Files.exists(newRoot)) {
            try {
                Files.createDirectories(newRoot);
            } catch (IOException e) {
                return "Failed to create directory for the new VoltDB root.";
            }
        } else if (!Files.isDirectory(newRoot)) {
            return "The path you specify as the VoltDB root does exist, but it doesn't look like a directory. "
                    + "Please check again before you run the command.";
        }
        return "Success";
    }
}
