/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

import com.google_voltpatches.common.base.Throwables;

/*
 * Extend the File class and override its constructors to allow a property to be specified
 * that places a prefix exactly one before all paths used by the process. Also
 * contains utility methods for manipulating files and directories and generating temporary
 * directories that are unlikely to collide with files from other users in /tmp
 *
 * This class should have not differ from File when used outside of test cases that set
 * the property or invoked initNewSubrootForThisProcess.
 */
public class VoltFile extends File {
    /*
     * The prefix to attach to the beginning of all paths
     */
    private static File m_voltFilePrefix = System.getProperty("VoltFilePrefix") != null ?
            new File (System.getProperty("VoltFilePrefix")) : null;

    /*
     * String of characters that is used to ensure that the prefix is only prepended
     * once
     */
    public static final String m_magic = "3909498365727147224L";

    /*
     * Generic root for this user to contain other subroots.
     * This is /tmp/${username}
     */
    public static final File m_root = new File(getRootUserPortion());


    public static void setSubrootForThisProcess(File prefix) {
        m_voltFilePrefix = prefix;
    }

    private static String getRootUserPortion() {
        return "/tmp/" + System.getProperty("user.name");
    }

    /*
     * Do some basic checking to make sure that the root for all the subroots
     * exists and can be manipulated
     */
    private static void ensureUserRootExists() throws IOException {
        if (!m_root.exists()) {
            if (!m_root.mkdir()) {
                throw new IOException("Unable to create \"" + m_root + "\"");
            }
        }
        if (!m_root.isDirectory()) {
            throw new IOException("\"" + m_root + "\" exists but is not a directory");
        }
        if (!m_root.canRead()) {
            throw new IOException("\"" + m_root + "\" exists but is not readable");
        }
        if (!m_root.canWrite()) {
            throw new IOException("\"" + m_root + "\" exists but is not writable");
        }
        if (!m_root.canExecute()) {
            throw new IOException("\"" + m_root + "\" exists but is not executable");
        }
    }

    /*
     * Create a "root filesystem" complete with its very own /tmp/${username}
     * to fool a process into thinking it has a private filesystem
     */
    public static File getNewSubroot() throws IOException {
        ensureUserRootExists();
        File tempFile = File.createTempFile("foo", "", m_root);
        if (!tempFile.delete()) {
            throw new IOException();
        }
        tempFile = new File(tempFile.getPath() + m_magic);
        if (!tempFile.mkdir()) {
            throw new IOException();
        }
        File tempDir = new File(tempFile, "tmp");
        if (!tempDir.mkdir()) {
            throw new IOException();
        }
        File tempUserDir = new File(tempDir, System.getProperty("user.name"));
        if (!tempUserDir.mkdir()) {
            throw new IOException();
        }
        return tempFile;
    }

    public static File getServerSpecificRoot(String hostId, boolean clearLocalDataDirectories) {
        try {
            ensureUserRootExists();
            //We need this not just number as DR uses 2 clusters and dont have to collide directories.
            File tempUserDir = new File(m_root, hostId + "-" + String.valueOf(System.nanoTime()));
            Thread.sleep(0, 1);
            if (!tempUserDir.isDirectory()) {
                if (!tempUserDir.mkdir()) {
                    return null;
                }
            }
            if (clearLocalDataDirectories) {
                recursivelyDelete(tempUserDir);
            }
            return tempUserDir;
        } catch (Exception ioe) {
            Throwables.propagate(ioe);
        }
        return null;
    }
    /*
     * Basic kill it with fire. Deletes everything in /tmp/${username} of
     * the actual filesystem (not one of the created subroots)
     */
    public static void deleteAllSubRoots() throws IOException {
        ensureUserRootExists();
        for (File f : m_root.listFiles()) {
            recursivelyDelete(f);
        }
    }

    /*
     * Merge one directory into another via copying. Useful for simulating
     * node removal or files being moved from node to node or duplicated.
     */
    public static void moveSubRootContents(File fromSubRoot, File toSubRoot) throws IOException {
        assert(fromSubRoot.exists() && fromSubRoot.isDirectory());
        assert(toSubRoot.exists() && toSubRoot.isDirectory());

        for (File file : fromSubRoot.listFiles()) {
            File fInOtherSubroot = new File(toSubRoot, file.getName());

            if (file.isDirectory()) {
                if (!fInOtherSubroot.exists()) {
                    if (!fInOtherSubroot.mkdir()) {
                        throw new IOException("Can't create directory " + fInOtherSubroot);
                    }
                }
                moveSubRootContents(file, fInOtherSubroot);
            }
            else {
                if (fInOtherSubroot.exists()) {
                    throw new IOException(fInOtherSubroot + " already exists");
                }
                if (!fInOtherSubroot.createNewFile()) {
                    throw new IOException();
                }

                FileInputStream fis = new FileInputStream(file);
                FileOutputStream fos = new FileOutputStream(fInOtherSubroot);
                FileChannel inputChannel = fis.getChannel();
                FileChannel outputChannel = fos.getChannel();
                BBContainer bufC = DBBPool.allocateDirect(8192);
                ByteBuffer buf = bufC.b();

                try {
                    while (inputChannel.read(buf) != -1) {
                        buf.flip();
                        outputChannel.write(buf);
                        buf.clear();
                    }
                }
                finally {
                    // These calls to close() also close the channels.
                    fis.close();
                    fos.close();
                    bufC.discard();
                }
            }
        }
    }

    /*
     * Tests that use ServerThread and LocalSingleProcessServer can't use the
     * property to set m_voltFilePrefix so they have to invoke this to make
     * sure a clean filesystem is generated.
     */
    public static File initNewSubrootForThisProcess() throws IOException {
        m_voltFilePrefix = getNewSubroot();
        return m_voltFilePrefix;
    }

    /*
     * Disable the prefixing functionality. Useful if there is a previous test case that has set
     * a subroot that needs to be cleaned up in tearDown()
     */
    public static void resetSubrootForThisProcess() throws IOException {
        m_voltFilePrefix = null;
    }

    public static void recursivelyDelete(File file, boolean deleteRoot) throws IOException {
        if (!file.exists()) {
            return;
        }

        if (deleteRoot) {
            recursivelyDelete(file);
        }
        else if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                recursivelyDelete(f);
            }
        }
    }

    /*
     * One of those why doesn't Java ship with this functions
     */
    public static void recursivelyDelete(File f) throws IOException {
        if (!f.exists()) {
            return;
        }
        if (f.isDirectory()) {
            for (File f1 : f.listFiles()) {
                recursivelyDelete(f1);
            }
            if (!f.delete()) {
                throw new IOException("Unable to delete directory " + f);
            }
        }
        else {
            if (!f.delete()) {
                throw new IOException("Unable to delete file " + f);
            }
        }
    }

    /**
     * Check if the given absolute path is a temp test path.
     * @param path    An absolute path
     * @return true if the path contains the magic string in it.
     */
    public static boolean isTestPath(final String path) {
        return path.contains(m_magic);
    }

    /**
     * Strip the magic temp test path prefix from the given path
     * if it is a test path, no-op if it's not.
     * @param path    An absolute path
     * @return A new absolute path with the temp test path prefix removed.
     */
    public static String removeTestPrefix(final String path) {
        if (isTestPath(path)) {
            return path.substring(m_voltFilePrefix.getAbsolutePath().length());
        }
        return path;
    }

    /*
     * These methods override file behavior and prefix a root path to all the files exactly once
     */
    public VoltFile(String pathname) {
        super(getFixedPathname(pathname));
    }

    /*
     * Given the requested pathname, come up with the altered one based on the value
     * of m_voltFilePrefix
     */
    private static String getFixedPathname(final String pathname) {
        if (pathname == null || m_voltFilePrefix == null) {
            return pathname;
        }
        if (pathname.contains(m_magic)) {
            if (pathname.contains(m_voltFilePrefix.getAbsolutePath())) {
                return pathname;
            }

            int offset = pathname.indexOf(m_magic) + m_magic.length();
            String relativePath = pathname.substring(offset);
            // The this is probably a snapshot path and needs to be re-mapped to our snapshot
            // directory because truncation snapshot requests specify absolute paths
            return m_voltFilePrefix.getAbsolutePath() + relativePath;
        }

        return m_voltFilePrefix + File.separator + pathname;
    }

    public VoltFile(String parent, String child) {
        super(getFixedPathname(parent), child);
    }

    public VoltFile(File parent, String child) {
        super(getFixedPathname(parent.getPath()), child);
    }

    public VoltFile(VoltFile parent, String child) {
        super(parent, child);
    }

    /**
     *
     */
    private static final long serialVersionUID = -3909498365727147224L;

}
