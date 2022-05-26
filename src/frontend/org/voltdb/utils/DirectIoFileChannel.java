/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.sun.nio.file.ExtendedOpenOption;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.unix.Errors;
import sun.misc.JavaIOFileDescriptorAccess;
import sun.misc.SharedSecrets;
import sun.nio.ch.FileChannelImpl;

public class DirectIoFileChannel {
    private static final VoltLogger s_log = new VoltLogger("HOST");
    // Open options for DIRECT IO support. Only exists in java 10+
    private static final Set<OpenOption> s_directOpenOptions;

    // Shared secret to set the file descriptor
    private static final JavaIOFileDescriptorAccess s_fdAccess;
    // Method which retrieves the raw byte representation of a unix path
    private static final Method s_getPathAsByteArray;

    static {
        // Little hack to load netty native libraries so that nice IOExceptions can be thrown
        Epoll.isAvailable();

        Set<OpenOption> openOptions = null;
        JavaIOFileDescriptorAccess fdAccess = null;
        Method getPathAsByteArray = null;

        try {
            OpenOption direct = ExtendedOpenOption.valueOf("DIRECT");
            openOptions = ImmutableSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE, direct);
        } catch (IllegalArgumentException e) {
            fdAccess = SharedSecrets.getJavaIOFileDescriptorAccess();
            try {
                Class<?> clazz = PosixAdvise.class.getClassLoader().loadClass("sun.nio.fs.UnixPath");
                getPathAsByteArray = clazz.getDeclaredMethod("asByteArray");
                getPathAsByteArray.setAccessible(true);
            } catch (ClassNotFoundException | NoSuchMethodException e1) {
                throw new RuntimeException("Unable to load java classes for direct IO", e1);
            }
        }

        s_directOpenOptions = openOptions;
        s_fdAccess = fdAccess;
        s_getPathAsByteArray = getPathAsByteArray;
    }

    /**
     * Utility method for testing if directIO is supported for {@code path}. If {@code path} is a directory then a
     * temporary file will be created inside of the directory. If {@code path} is not a file it will be created and then
     * deleted. If {@code path} is a file then it will be opened and closed.
     *
     * @param path being tested
     * @return {@code true} if directIO is supported
     * @throws IllegalArgumentException if {@code path}'s parent directory does not exist or is a non regular file
     */
    public static boolean supported(Path path) throws IllegalArgumentException {
        Path file = path;
        boolean delete = true;

        if (Files.isDirectory(path)) {
            file = Paths.get(path.toString(), "directIOTest-" + System.nanoTime());
        } else if (Files.isRegularFile(path)) {
            delete = false;
        } else if (Files.exists(path)) {
            throw new IllegalArgumentException("Path exists but is not a regular file or directory: " + path);
        } else if (!Files.isDirectory(path.getParent())) {
            throw new IllegalArgumentException("Parent directory does not exist: " + path);
        }

        try (FileChannel fc = open(file)) {
            if (delete) {
                Files.delete(file);
            }
            return true;
        } catch (IOException | IllegalArgumentException e) {
            if (s_log.isDebugEnabled()) {
                s_log.debug("Path does not support direct IO: " + path, e);
            }
            return false;
        }
    }

    /**
     * Open and create a new direct IO file channel if it does not exist. If the file already it will not be truncated
     * and file position will be at 0.
     *
     * @param path to file
     * @return {@link FileChannel} opened to {@code path}
     * @throws IOException if there was an error while opening
     */
    public static FileChannel open(Path path) throws IOException {
        if (s_directOpenOptions != null) {
            return FileChannel.open(path, s_directOpenOptions);
        }

        // This is a rudimentary approximation of what java does to open a channel with direct IO enabled
        try {
            int fd = nativeOpen((byte[]) s_getPathAsByteArray.invoke(path));
            if (fd < 0) {
                // This method is guaranteed to throw so this should never get to the return null
                Errors.ioResult(DirectIoFileChannel.class.getName() + ".open(Path)", fd);
                return null;
            }

            FileDescriptor fileDescriptor = new FileDescriptor();
            s_fdAccess.set(fileDescriptor, fd);
            return FileChannelImpl.open(fileDescriptor, path.toString(), false, true, null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e);
        }
    }

    private static native int nativeOpen(byte[] path);

    private DirectIoFileChannel() {}
}
