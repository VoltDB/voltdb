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

import org.voltdb.logging.VoltLogger;

public class EELibraryLoader {

    private static boolean voltSharedLibraryLoaded = false;

    private static final VoltLogger hostLog = new VoltLogger("HOST");


    static private boolean test64bit() {
        // Sun JVMs are nice and chatty on this topic.
        String sun_arch_data_model = System.getProperty("sun.arch.data.model", "none");
        if (sun_arch_data_model.contains("64")) {
            return true;
        }
        hostLog.info("Unable to positively confirm a 64-bit JVM. VoltDB requires" +
                " a 64-bit JVM. A 32-bit JVM will fail to load the native VoltDB" +
                " library.");
        return false;
    }

    /**
     * Load the shared native library if not yet loaded. Returns true if the library was loaded
     **/
    public synchronized static boolean loadExecutionEngineLibrary(boolean mustSuccede) {
        if (!voltSharedLibraryLoaded) {
            if (VoltDB.getLoadLibVOLTDB()) {
                test64bit();

                try {
                    final VoltLogger hostLog = new VoltLogger("HOST");
                    final String libname = "voltdb-" + VoltDB.instance().getVersionString();
                    hostLog.info("Attempting to load native VoltDB library " + libname +
                            ". Expect to see a confirmation following this upon success. " +
                            "If none appears then you may need to compile VoltDB for your platform " +
                            "or you may be running a 32-bit JVM.");
                    System.loadLibrary(libname);
                    voltSharedLibraryLoaded = true;
                    hostLog.info("Successfully loaded native VoltDB library " + libname +
                    ".");
                } catch (Throwable e) {
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
