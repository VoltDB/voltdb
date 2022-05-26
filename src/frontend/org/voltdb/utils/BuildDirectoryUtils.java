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
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.compiler.VoltCompiler;

/**
 * This class just lets you get a PrintStream backed by a file
 * in a standard-ish place. It will create the directory structure
 * if it doesn't exist.
 *
 */
public abstract class BuildDirectoryUtils {

    public static final String userRootPrefix = "statement-plans/";
    public static final String debugRootPrefix = "debugoutput/";

    static String m_debugRoot = null;
    static String m_userRoot = null;
    static Set<String> m_seenPaths = new TreeSet<String>();

    /**
     * Write a file to disk during compilation that has some neato info generated during compilation.
     * If the debug flag is true, that means this file should only be written if the compiler is
     * running in debug mode.
     */
    public static void writeFile(final String dir, final String filename, String content, boolean debug) {
        // skip debug files when not in debug mode
        if (debug && !VoltCompiler.DEBUG_MODE) {
            return;
        }

        // cache the root of the folder for the debugoutput and the statement-plans folder
        if (m_debugRoot == null) {
            if (System.getenv("TEST_DIR") != null) {
                m_debugRoot = System.getenv("TEST_DIR") + File.separator + debugRootPrefix;
            } else {
                m_debugRoot = debugRootPrefix;
            }
        }
        if (m_userRoot == null) {
            if (System.getenv("TEST_DIR") != null) {
                m_userRoot = System.getenv("TEST_DIR") + File.separator + userRootPrefix;
            } else {
                m_userRoot = userRootPrefix;
            }
        }

        // pic a place for the file based on debugness of the file in question
        String root = debug ? m_debugRoot : m_userRoot;

        // don't call mkdirs more than once per subdir, so keep a cache
        String subFolderPath = root;
        if (dir != null) {
            subFolderPath += File.separator + dir;
        }
        if (!m_seenPaths.contains(subFolderPath)) {
            File f = new File(subFolderPath);
            f.mkdirs();
            m_seenPaths.add(subFolderPath);
        }

        String filepath = subFolderPath + File.separator + filename;
        File f = new File(filepath);
        PrintStream streamOut = null;
        try {
            streamOut = new PrintStream(f);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        streamOut.println(content);
        streamOut.close();
    }

    public static String getBuildDirectoryPath() {
        String envPath = System.getenv("TEST_DIR");
        if (envPath != null) {
            File path = new File(envPath);
            path.mkdirs();
            if (!path.exists() || !path.isDirectory() || !path.canRead() || !path.canWrite() || !path.canExecute()) {
                throw new RuntimeException("Could not create test directory");
            }
            return envPath;
        } else {
            return ".";
        }
    }

}
