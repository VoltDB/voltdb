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

package org.voltdb;

import org.voltcore.logging.VoltLogger;

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
                    String versionString = VoltDB.instance().getEELibraryVersionString();
                    // this fallback is for test code only
                    if (versionString == null) {
                        versionString = VoltDB.instance().getVersionString();
                    }
                    assert(versionString != null);
                    final String libname = "voltdb-" + versionString;
                    hostLog.info("Loading native VoltDB code ("+libname+"). A confirmation message will follow if the loading is successful.");
                    System.loadLibrary(libname);
                    voltSharedLibraryLoaded = true;
                    hostLog.info("Successfully loaded native VoltDB library " + libname + ".");
                } catch (Throwable e) {
                    if (mustSuccede) {
                        String msg = "Library VOLTDB JNI shared library loading failed. Library path " +
                                System.getProperty("java.library.path") + "\n";
                        msg += "The library may have failed to load because it can't be found in your " +
                                "load library path, or because it is not compatible with the current " +
                                "platform.\n";
                        msg +=  "VoltDB provides builds on our website for 64-bit OSX systems >= 10.6, " +
                                "and 64-bit Linux systems with kernels >= 2.6.18.";
                        VoltDB.crashLocalVoltDB(msg, false, null);
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
