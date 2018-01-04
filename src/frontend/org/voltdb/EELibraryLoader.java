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

package org.voltdb;

import static org.voltcore.common.Constants.VOLT_TMP_DIR;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.voltcore.logging.VoltLogger;

public class EELibraryLoader {

    public static final String USE_JAVA_LIBRARY_PATH = "use.javalib";
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
    public synchronized static boolean loadExecutionEngineLibrary(boolean mustSucceed) {
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
                    if (Boolean.getBoolean(USE_JAVA_LIBRARY_PATH)) {
                        System.loadLibrary(libname);
                    } else {
                        File libFile = getNativeLibraryFile(libname);
                        System.load(libFile.getAbsolutePath());
                    }
                    voltSharedLibraryLoaded = true;
                    hostLog.info("Successfully loaded native VoltDB library " + libname + ".");
                } catch (Throwable e) {
                    if (hostLog.isDebugEnabled()) {
                        hostLog.debug("Error loading VoltDB JNI shared library", e);
                    }
                    if (mustSucceed) {
                        String msg = "Library VOLTDB JNI shared library loading failed with error: " + e.getMessage() + "\n";
                        msg += "Library path " + System.getProperty("java.library.path") + ", " +
                               USE_JAVA_LIBRARY_PATH + "=" + System.getProperty(USE_JAVA_LIBRARY_PATH) + "\n";
                        msg += "The library may have failed to load because it can't be found in your " +
                                "load library path, or because it is not compatible with the current " +
                                "platform.\n";
                        msg +=  "VoltDB provides builds on our website for 64-bit OSX systems >= 10.6, " +
                                "and 64-bit Linux systems with kernels >= 2.6.18.";
                        if (e instanceof UnsatisfiedLinkError) {
                            msg += "\nOr the library may have failed to load because java.io.tmpdir should be set to a different directory. " +
                                   "Use VOLTDB_OPTS='-Djava.io.tmpdir=<dirpath>' to set it.";
                        }
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

    /*
     * Returns the native library file copied into a readable location.
     */
    private static File getNativeLibraryFile(String libname) {

        // for now, arch is always x86_64
        String pathFormat = "/org/voltdb/native/%s/x86_64";
        String libPath = null;
        if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            libPath = String.format(pathFormat, "Mac");
        } else {
            libPath = String.format(pathFormat, "Linux");
        }

        String libFileName = System.mapLibraryName(libname);
        if (EELibraryLoader.class.getResource(libPath + "/" + libFileName) == null) {
            // mapLibraryName does not give us the correct name on mac sometimes
            if (System.getProperty("os.name").toLowerCase().contains("mac")) {
                libFileName = "lib" + libname + ".jnilib";
            }
            if (EELibraryLoader.class.getResource(libPath + "/" + libFileName) == null) {
                String msg = "Could not find library resource using path: " + libPath + "/" + libFileName;
                hostLog.warn(msg);
                throw new RuntimeException(msg);
            }
        }

        File tmpFilePath = new File(System.getProperty(VOLT_TMP_DIR, System.getProperty("java.io.tmpdir")));
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("Temp directory to which shared libs are extracted is: " + tmpFilePath.getAbsolutePath());
        }
        try {
            return loadLibraryFile(libPath, libFileName, tmpFilePath.getAbsolutePath());
        } catch(IOException e) {
            hostLog.error("Error loading Volt library file from jar", e);
            throw new RuntimeException(e);
        }
    }

    private static File loadLibraryFile(String libFolder, String libFileName, String tmpFolder) throws IOException {
        hostLog.debug("Loading library from jar using path=" + libFolder + "/" + libFileName);

        // Using UUID as in Snappy, but we probably don't need it?
        String uuid = UUID.randomUUID().toString();
        String extractedLibFileName = String.format("voltdb-%s-%s", uuid, libFileName);
        File extractedLibFile = new File(tmpFolder, extractedLibFileName);

        String libPath = libFolder + "/" + libFileName;
        // Extract a native library file into the target directory
        InputStream reader = null;
        FileOutputStream writer = null;
        try {
            reader = EELibraryLoader.class.getResourceAsStream(libPath);
            try {
                writer = new FileOutputStream(extractedLibFile);

                byte[] buffer = new byte[8192];
                int bytesRead = 0;
                while ((bytesRead = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, bytesRead);
                }
            }
            finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }

            // Delete the extracted lib file on JVM exit.
            extractedLibFile.deleteOnExit();
        }

        // Set executable (x) flag to enable Java to load the native library
        boolean success = extractedLibFile.setReadable(true) &&
                extractedLibFile.setWritable(true, true) &&
                extractedLibFile.setExecutable(true);
        if (!success) {
            String msg = "Could not update extracted lib file " + extractedLibFile + " to be rwx";
            hostLog.warn(msg);
            throw new RuntimeException(msg);
        }

        return new File(tmpFolder, extractedLibFileName);
    }
}
