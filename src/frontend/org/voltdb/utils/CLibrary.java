/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.logging.VoltLogger;
import com.sun.jna.Native;

public class CLibrary {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    static {
        try {
            Native.register("c");
        } catch (Exception e) {
            hostLog.warn("Failed to load libc via JNA", e);
        }
    }

    public static final class Rlimit extends  com.sun.jna.Structure {
        public long rlim_cur = 0;
        public long rlim_max = 0;
    }

    public static final int RLIMIT_NOFILE_LINUX = 7;
    public static final int RLIMIT_NOFILE_MAC_OS_X = 8;

    public static native final int getrlimit(int resource, Rlimit rlimit);

    /*
     * Returns the limit on the number of open files or null
     * on failure
     */
    public static Integer getOpenFileLimit() {
        try {
            Rlimit rlimit = new Rlimit();
            int retval =
                getrlimit(
                    System.getProperty("os.name").equals("Linux") ? RLIMIT_NOFILE_LINUX : RLIMIT_NOFILE_MAC_OS_X,
                            rlimit);
            if (retval != 0) {
                return null;
            } else if (rlimit.rlim_cur >= 1024) {
                //Seems to be a sensible value that is the default or greater
                return (int)rlimit.rlim_cur;
            } else {
                return null;
            }
        } catch (Exception e) {
            hostLog.warn("Failed to retrieve open file limit via JNA", e);
        }
        return null;
    }
}
