/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class just lets you get a PrintStream backed by a file
 * in a standard-ish place. It will create the directory structure
 * if it doesn't exist.
 *
 */
public abstract class BuildDirectoryUtils {

    public static final String rootPath = "debugoutput/";

    static String m_debugRoot = null;
    static Set<String> m_seenPaths = new TreeSet<String>();

    public static PrintStream getDebugOutputPrintStream(final String dir, final String filename) {
        // cache the root of the folder
        if (m_debugRoot == null) {
            if (System.getenv("TEST_DIR") != null) {
                m_debugRoot = System.getenv("TEST_DIR") + File.separator + rootPath;
            } else {
                m_debugRoot = rootPath;
            }
        }

        // don't call mkdirs more than once per subdir, so keep a cache
        String subFolderPath = m_debugRoot + File.separator + dir;
        if (!m_seenPaths.contains(subFolderPath)) {
            File f = new File(subFolderPath);
            f.mkdirs();
            m_seenPaths.add(subFolderPath);
        }

        String filepath = subFolderPath + File.separator + filename;
        File f = new File(filepath);
        try {
            return new PrintStream(f);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String getBuildDirectoryPath() {
        String envPath = System.getenv("TEST_DIR");
        if (envPath != null) {
            return envPath;
        } else {
            return ".";
        }
    }
}
