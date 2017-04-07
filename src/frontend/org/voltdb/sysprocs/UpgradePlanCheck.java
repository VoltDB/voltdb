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

public class UpgradePlanCheck extends VoltNTSystemProcedure {

    private final static int MINIMUM_MAJOR_VERSION = 7;
    private final static int MINIMUM_MINOR_VERSION = 2;
    public VoltTable run(String newKitPath, String newRootPath) throws InterruptedException, ExecutionException {

        Path newKit = Paths.get(newKitPath);
        if (Files.exists(newKit) && Files.isDirectory(newKit)) {
            try {
                String version = new String(Files.readAllBytes(Paths.get(newKitPath, "version.txt")));
                String[] versionNumber = version.split(".");
                if (versionNumber.length < 2) {
                    //TODO: error
                }
                Integer majorVersion = Integer.getInteger(versionNumber[0]);
                Integer minorVersion = Integer.getInteger(versionNumber[1]);
                if (majorVersion != null && minorVersion != null &&
                        (majorVersion < MINIMUM_MAJOR_VERSION ||
                                (majorVersion == MINIMUM_MAJOR_VERSION && minorVersion < MINIMUM_MINOR_VERSION))) {
                    //TODO: error
                }
            } catch (IOException e) {
                //TODO: error
            }
        }

        Path newRoot = Paths.get(newRootPath);
        if (Files.exists(newRoot) && Files.isDirectory(newRoot)) {

        }

        VoltTable vt = new VoltTable(
                new ColumnInfo[] { new ColumnInfo("SYSTEM_GC_DURATION_NANOS", VoltType.BIGINT) });
        vt.addRow(duration);

        return vt;
    }
}
