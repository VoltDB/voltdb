/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.apache.log4j.Logger;
import org.voltdb.utils.VoltLoggerFactory;

public class EELibraryLoader {

    /**
     * Public because DBBPool might also load the shared library and needs to check this value.
     */
    private static boolean voltSharedLibraryLoadAttempted = false;

    private static boolean voltSharedLibraryLoaded = false;

    private static final Logger hostLog = Logger.getLogger(
            "HOST", VoltLoggerFactory.instance());

    /**
     * Load the shared native library if not yet loaded. Returns true if the library was loaded
     **/
    public synchronized static boolean loadExecutionEngineLibrary(boolean mustSuccede) {
        if (!voltSharedLibraryLoadAttempted) {
            voltSharedLibraryLoadAttempted = true;
            if (VoltDB.getLoadLibVOLTDB()) {
                try {
                    final Logger hostLog = Logger.getLogger("HOST", VoltLoggerFactory.instance());
                    final String libname = "voltdb-" + VoltDB.instance().getVersionString();
                    hostLog.info("Attempting to load native VoltDB library " + libname +
                            ". Expect to see a confirmation following this upon success. " +
                            "If none appears then you may need to compile VoltDB for your platform");
                    System.loadLibrary(libname);
                    voltSharedLibraryLoaded = true;
                    hostLog.info("Successfully loaded native VoltDB library " + libname +
                    ".");
                } catch (Throwable t) {
                    if (mustSuccede) {
                        hostLog.fatal("Library VOLTDB JNI shared library loading failed. Library path "
                                + System.getProperty("java.library.path"));
                        VoltDB.crashVoltDB();
                    } else {
                        hostLog.info("Library VOLTDB JNI shared library loading failed. Library path "
                                + System.getProperty("java.library.path"));
                    }
                    return false;
                }
            } else {
                return false;
            }
        }
        return voltSharedLibraryLoaded;
    }
}
