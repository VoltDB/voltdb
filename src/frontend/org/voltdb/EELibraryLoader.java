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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.voltcore.logging.VoltLogger;

public class EELibraryLoader {

    private static final String USE_JAVA_LIBRARY_PATH = "use.javalib";
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
                    File libFile = getNativeLibraryFile(libname);
                    if (libFile==null) {
                        System.loadLibrary(libname);
                    } else {
                        System.load(libFile.getAbsolutePath());
                    }
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

    /*
     * Returns the native library file copied into a readable location, if the library from volt jar
     * should be used. Returns null otherwise to default to java.library.path as fall back.
     */
    private static File getNativeLibraryFile(String libname) {
        if (Boolean.parseBoolean(System.getProperty(USE_JAVA_LIBRARY_PATH, "false"))) {
            return null;
        }

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
                hostLog.warn("Could not find library resource using path: " + libPath + "/" + libFileName +
                        ". Falling back to using java.library.path");
                return null;
            }
        }

        File tmpFilePath = new File(System.getProperty("java.io.tmpdir"));
        try {
            return loadLibraryFile(libPath, libFileName, tmpFilePath.getAbsolutePath());
        } catch(IOException e) {
            hostLog.error("Error loading library file from jar. Falling back to using java.library.path", e);
            return null;
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
            hostLog.warn("Could not update extracted lib file " + extractedLibFile +
                    " to be rwx. Falling back to using java.library.path");
            return null;
        }

        /* Do we need this check that is in SnappyLoader?
            // Check whether the contents are properly copied from the resource folder
            {
                InputStream nativeIn = null;
                InputStream extractedLibIn = null;
                try {
                    nativeIn = EELibraryLoader.class.getResourceAsStream(libPath);
                    extractedLibIn = new FileInputStream(extractedLibFile);

                    if (!contentsEquals(nativeIn, extractedLibIn)) {
                        throw new SnappyError(SnappyErrorCode.FAILED_TO_LOAD_NATIVE_LIBRARY, String.format("Failed to write a native library file at %s", extractedLibFile));
                    }
                }
                finally {
                    if (nativeIn != null) {
                        nativeIn.close();
                    }
                    if (extractedLibIn != null) {
                        extractedLibIn.close();
                    }
                }
            }
         */

        return new File(tmpFolder, extractedLibFileName);
    }
}
