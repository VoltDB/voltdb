/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import com.sun.jna.Native;
import sun.misc.SharedSecrets;

import java.io.FileDescriptor;

public class PosixAdvise {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

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

    public static long fadvise(FileDescriptor fd, long offset, long size, int advice) {
        final long filedescriptor = SharedSecrets.getJavaIOFileDescriptorAccess().get(fd);
        return fadvise(filedescriptor, offset, size, advice);
    }

    public static native long fadvise(long fd, long offset, long size, int advice);

    public static native long fallocate(long fd, long offset, long size);
    public static long fallocate(FileDescriptor fd, long offset, long size) {
        final long filedescriptor = SharedSecrets.getJavaIOFileDescriptorAccess().get(fd);
        return fallocate(filedescriptor, offset, size);
    }
}
