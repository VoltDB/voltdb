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
import org.apache.commons.io.FileUtils;

/*
 * DO NOT USE THIS CLASS OUTSIDE OF Snapshot code unless you have similar requirements.
 * Extend the File class for snapshot files only dont use this for any other files.
 * this places a prefix exactly one before all paths used by the process. Also
 * contains utility methods for manipulating files and directories and generating temporary
 * directories that are unlikely to collide with files from other users in /tmp
 *
 * This class should have not differ from File when used outside of test cases.
 */
public class VoltSnapshotFile extends File {
    /*
     * The prefix to attach to the beginning of all paths
     */
    private static File m_voltSnapshotFilePrefix = System.getProperty("VoltSnapshotFilePrefix") != null ?
            new File (System.getProperty("VoltSnapshotFilePrefix")) : null;

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
        m_voltSnapshotFilePrefix = prefix;
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
                FileUtils.cleanDirectory(tempUserDir);
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
        FileUtils.cleanDirectory(m_root);
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
     * These methods override file behavior and prefix a root path to all the files exactly once
     */
    public VoltSnapshotFile(String pathname) {
        super(getFixedPathname(pathname));
    }

    /*
     * Given the requested pathname, come up with the altered one based on the value
     * of m_voltSnapshotFilePrefix
     */
    private static String getFixedPathname(final String pathname) {
        if (pathname == null || m_voltSnapshotFilePrefix == null) {
            return pathname;
        }

        return m_voltSnapshotFilePrefix + File.separator + pathname;
    }

    public VoltSnapshotFile(String parent, String child) {
        super(getFixedPathname(parent), child);
    }

    public VoltSnapshotFile(File parent, String child) {
        super(getFixedPathname(parent.getPath()), child);
    }

    public VoltSnapshotFile(VoltSnapshotFile parent, String child) {
        super(parent, child);
    }

    public static void setVoltSnapshotFilePrefix(String prefix) {
        VoltSnapshotFile.m_voltSnapshotFilePrefix = new File(prefix);
    }

    private static final long serialVersionUID = -3909498365727147224L;

}
