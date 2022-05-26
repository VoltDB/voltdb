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

import java.io.FileDescriptor;

import org.voltcore.logging.VoltLogger;

public class PosixAdvise {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final boolean FALLOCATE_SUPPORTED;
    public static final boolean SYNC_FILE_RANGE_SUPPORTED;
    public static final boolean ENABLE_FADVISE_DONTNEED;
    static {
        SYNC_FILE_RANGE_SUPPORTED = System.getProperty("os.name").equalsIgnoreCase("linux") ;
        FALLOCATE_SUPPORTED = System.getProperty("os.name").equalsIgnoreCase("linux") ;
        ENABLE_FADVISE_DONTNEED = Boolean.getBoolean("ENABLE_FADVISE_DONTNEED");
    }

    /*
     * madvise
     */
    public static final int POSIX_MADV_NORMAL = 0;
    public static final int POSIX_MADV_RANDOM = 1;
    public static final int POSIX_MADV_SEQUENTIAL = 2;
    public static final int POSIX_MADV_WILLNEED = 3;
    public static final int POSIX_MADV_DONTNEED = 4;
    public static native long madvise(long addr, long size, int advice);


    /*
     * fadvise
     */
    public static final int POSIX_FADV_NORMAL = 0;
    public static final int POSIX_FADV_RANDOM = 1;
    public static final int POSIX_FADV_SEQUENTIAL = 2;
    public static final int POSIX_FADV_WILLNEED = 3;
    public static final int POSIX_FADV_DONTNEED = 4;

    /*
     * sync_file_range flags
     */
    public static final int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
    public static final int SYNC_FILE_RANGE_WRITE = 2;
    public static final int SYNC_FILE_RANGE_WAIT_AFTER = 4;
    //Convenience constant for the commonly used scenario
    public static final int SYNC_FILE_RANGE_SYNC =
            SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;

    public static native long nativeFadvise(FileDescriptor fd, long offset, long size, int advice);
    public static long fadvise(FileDescriptor fd, long offset, long size, int advice) {
        if (advice == POSIX_FADV_DONTNEED && !ENABLE_FADVISE_DONTNEED) return 0;
        return nativeFadvise(fd, offset, size, advice);
    }



    public static native long fallocate(FileDescriptor fd, long offset, long size);

    /*
     * Be aware sync_file_range does not make data durable. It doesn't handle ordering with metadata
     * nor does it emit write barriers
     */
    public static native long sync_file_range(FileDescriptor fd, long offset, long size, int flags);
}
