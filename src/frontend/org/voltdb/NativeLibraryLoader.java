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

package org.voltdb;

import static org.voltcore.common.Constants.VOLT_TMP_DIR;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;

import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.collect.Sets;

public class NativeLibraryLoader {

    public static final String USE_JAVA_LIBRARY_PATH = "use.javalib";
    private static final Set<String> s_loadedLibs = Sets.newHashSet();
    private static final VoltLogger s_hostLog = new VoltLogger("HOST");

    static private boolean test64bit() {
        // Sun JVMs are nice and chatty on this topic.
        String sun_arch_data_model = System.getProperty("sun.arch.data.model", "none");
        if (sun_arch_data_model.contains("64")) {
            return true;
        }
        s_hostLog.info("Unable to positively confirm a 64-bit JVM. VoltDB requires" +
                " a 64-bit JVM. A 32-bit JVM will fail to load the native VoltDB" +
                " library.");
        return false;
    }

    public synchronized static boolean loadVoltDB() {
        return load("voltdb", true, Boolean.getBoolean(USE_JAVA_LIBRARY_PATH));
    }

    public synchronized static boolean loadVoltDB(boolean mustSucceed) {
        return load("voltdb", mustSucceed, Boolean.getBoolean(USE_JAVA_LIBRARY_PATH));
    }

    public synchronized static boolean loadCatalogAPIs() {
        return load("catalog", true, true);
    }

    /**
     * Load VoltDB shared native libraries if not yet loaded.
     * @param name the library name.
     * @param mustSucceed whether a loading failure is fatal.
     * @param useJavaLib whether load the library from the system library location.
     * @return true if the library was loaded.
     */
    private static boolean load(String name, boolean mustSucceed, boolean useJavaLib) {
        if (s_loadedLibs.contains(name)) {
            return true;
        }
        if (! VoltDB.getLoadLibVOLTDB()) {
            return false;
        }
        test64bit();
        StringBuilder msgBuilder = new StringBuilder("Loading VoltDB native library ");
        String fullLibName = name;
        try {
            String versionString = VoltDB.instance().getEELibraryVersionString();
            // This fallback is for test code only.
            if (versionString == null) {
                versionString = VoltDB.instance().getVersionString();
            }
            assert(versionString != null);
            fullLibName = name + "-" + versionString;
            msgBuilder.append(fullLibName);
            File libFile = null;
            if (useJavaLib) {
                msgBuilder.append(" from the system library location. ");
            } else {
                libFile = getNativeLibraryFile(fullLibName);
                msgBuilder.append(" from file ");
                msgBuilder.append(libFile.getAbsolutePath());
                msgBuilder.append(". ");
            }
            msgBuilder.append("A confirmation message will follow if the loading is successful.");
            s_hostLog.info(msgBuilder.toString());
            if (useJavaLib) {
                System.loadLibrary(fullLibName);
            } else {
                System.load(libFile.getAbsolutePath());
            }
            s_loadedLibs.add(name);
            s_hostLog.info("Successfully loaded VoltDB native library " + fullLibName + ".");
            return true;
        } catch (Throwable e) {
            if (s_hostLog.isDebugEnabled()) {
                s_hostLog.debug("Error loading VoltDB JNI shared library", e);
            }
            if (useJavaLib) {
                s_hostLog.info("Retry loading from file.");
                return load(name, mustSucceed, false);
            }
            if (mustSucceed) {
                msgBuilder.setLength(0);
                msgBuilder.append("Failed to load shared library ").append(fullLibName).append(": ");
                msgBuilder.append(e.getMessage()).append('\n');
                msgBuilder.append("Library path: ").append(System.getProperty("java.library.path")).append('\n');
                msgBuilder.append("The library may have failed to load because it cannot be found in your ");
                msgBuilder.append("load library path, or because it is not compatible with the current platform.\n");
                msgBuilder.append("VoltDB provides builds on our website for 64-bit OS X systems >= 10.6, ");
                msgBuilder.append("and 64-bit Linux systems with kernels >= 2.6.18.");
                if (e instanceof UnsatisfiedLinkError) {
                    msgBuilder.append("\nOr the library may have failed to load because java.io.tmpdir should be set ");
                    msgBuilder.append("to a different directory. Use VOLTDB_OPTS='-Djava.io.tmpdir=<dirpath>' to set it.");
                }
                VoltDB.crashLocalVoltDB(msgBuilder.toString(), false, null);
            } else {
                s_hostLog.info("Failed to load shared library " + fullLibName + "\nLibrary path: "
                        + System.getProperty("java.library.path"));
            }
            return false;
        }
    }


    /**
     * Returns the native library file copied into a readable location.
     * @param libname the library name.
     * @return the native library file copied into a readable location.
     */
    private static File getNativeLibraryFile(String libname) {

        // for now, arch is always x86_64
        String pathFormat = "/org/voltdb/native/%s/x86_64";
        String libPath = null;
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("mac")) {
            libPath = String.format(pathFormat, "Mac");
        } else if (osName.contains("linux")) {
            libPath = String.format(pathFormat, "Linux");
        } else {
            throw new RuntimeException("Unsupported system: " + osName);
        }

        String libFileName = System.mapLibraryName(libname);
        if (NativeLibraryLoader.class.getResource(libPath + "/" + libFileName) == null) {
            // mapLibraryName does not give us the correct name on mac sometimes
            if (osName.contains("mac")) {
                libFileName = "lib" + libname + ".jnilib";
            }
            if (NativeLibraryLoader.class.getResource(libPath + "/" + libFileName) == null) {
                String msg = "Could not find library resource using path: " + libPath + "/" + libFileName;
                s_hostLog.warn(msg);
                throw new RuntimeException(msg);
            }
        }

        File tmpFilePath = new File(System.getProperty(VOLT_TMP_DIR, System.getProperty("java.io.tmpdir")));
        if (s_hostLog.isDebugEnabled()) {
            s_hostLog.debug("Temp directory to which shared libs are extracted is: " + tmpFilePath.getAbsolutePath());
        }
        try {
            return loadLibraryFile(libPath, libFileName, tmpFilePath.getAbsolutePath());
        } catch(IOException e) {
            s_hostLog.error("Error loading Volt library file from jar", e);
            throw new RuntimeException(e);
        }
    }

    private static File loadLibraryFile(String libFolder, String libFileName, String tmpFolder) throws IOException {
        s_hostLog.debug("Loading library from jar using path = " + libFolder + "/" + libFileName);

        // Using UUID as in Snappy, but we probably don't need it?
        String uuid = UUID.randomUUID().toString();
        String extractedLibFileName = String.format("voltdb-%s-%s", uuid, libFileName);
        File extractedLibFile = new File(tmpFolder, extractedLibFileName);

        String libPath = libFolder + "/" + libFileName;
        // Extract a native library file into the target directory
        try (InputStream reader = NativeLibraryLoader.class.getResourceAsStream(libPath)) {
            try (FileOutputStream writer = new FileOutputStream(extractedLibFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead = 0;
                while ((bytesRead = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, bytesRead);
                }
            }
        } finally {
            // Delete the extracted lib file on JVM exit.
            extractedLibFile.deleteOnExit();
        }

        // Set executable (x) flag to enable Java to load the native library
        boolean success = extractedLibFile.setReadable(true) &&
                extractedLibFile.setWritable(true, true) &&
                extractedLibFile.setExecutable(true);
        if (! success) {
            String msg = "Could not update extracted lib file " + extractedLibFile + " to be rwx";
            s_hostLog.warn(msg);
            throw new RuntimeException(msg);
        }

        return new File(tmpFolder, extractedLibFileName);
    }
}
