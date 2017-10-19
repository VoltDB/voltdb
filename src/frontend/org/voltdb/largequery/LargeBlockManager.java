/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
package org.voltdb.largequery;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.voltdb.utils.VoltFile;

/**
 * A class that manages large blocks produced by large queries.
 *
 * For now, this class has only one instance that's created on
 * start up in RealVoltDB.
 *
 * This class is also responsible for managing the files in the
 * directory large_query_swap under voltdbroot.
 */
public class LargeBlockManager {
    private final Path m_largeQuerySwapPath;
    private final Map<Long, Path> m_blockPathMap = new HashMap<>();
    private final Object m_accessLock = new Object();

    private static LargeBlockManager INSTANCE = null;

    private final static Set<OpenOption> OPEN_OPTIONS = new HashSet<>();
    private final static FileAttribute<Set<PosixFilePermission>> PERMISSIONS;

    static {
        OPEN_OPTIONS.add(StandardOpenOption.CREATE_NEW);
        OPEN_OPTIONS.add(StandardOpenOption.WRITE);
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
        PERMISSIONS = PosixFilePermissions.asFileAttribute(perms);
    }

    /**
     * Create the singleton instance of LargeBlockManager
     */
    public static synchronized void initializeInstance(Path largeQuerySwapPath) {
        if (INSTANCE != null) {
            throw new IllegalStateException("Attempt to re-initialize singleton large block manager");
        }

        INSTANCE = new LargeBlockManager(largeQuerySwapPath);
    }

    /**
     * Get the singleton instance of LargeBlockManager
     */
    public static LargeBlockManager getInstance() {
        return INSTANCE;
    }

    /**
     * Private constructor---use initializeInstance and getInstance instead.
     */
    private LargeBlockManager(Path largeQuerySwapPath) {
        m_largeQuerySwapPath = largeQuerySwapPath;
    }

    /**
     * Cleans out the large_query_swap directory of all files.
     * Large query intermediate storage and results do not persist
     * across shutdown/startup.
     * This method is to clean up any spurious files that may not have
     * been deleted due to an unexpected shutdown.
     * @throws IOException
     */
    public void clearSwapDir() throws IOException {
        if (! m_blockPathMap.isEmpty()) {
            throw new IllegalStateException("Attempt to clear swap directory when "
                    + "there are still managed blocks; use releaseAllBlocks() instead");
        }

        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(m_largeQuerySwapPath)) {
            Iterator<Path> it = dirStream.iterator();
            while (it.hasNext()) {
                Path path = it.next();
                VoltFile.recursivelyDelete(path.toFile());
            }
        }
    }

    /**
     * Store the given block with the given ID to disk.
     * @param id     the ID of the block
     * @param block  the bytes for the block
     * @throws IOException
     */
    public void storeBlock(long id, ByteBuffer block) throws IOException {
        synchronized (m_accessLock) {
            if (m_blockPathMap.containsKey(id)) {
                throw new IllegalArgumentException("Request to store block that is already stored: " + id);
            }

            int origPosition = block.position();
            block.position(0);
            Path blockPath = makeBlockPath(id);
            try (SeekableByteChannel channel = Files.newByteChannel(blockPath, OPEN_OPTIONS, PERMISSIONS)) {
                channel.write(block);
            }
            finally {
                block.position(origPosition);
            }

            m_blockPathMap.put(id, blockPath);
        }
    }

    /**
     * Read the block with the given ID into the given byte buffer.
     * @param id      ID of the block to load
     * @param block   The block to write the bytes to
     * @throws IOException
     */
    public void loadBlock(long id, ByteBuffer block) throws IOException {
        synchronized (m_accessLock) {
            if (! m_blockPathMap.containsKey(id)) {
                throw new IllegalArgumentException("Request to load block that is not stored: " + id);
            }

            int origPosition = block.position();
            block.position(0);
            Path blockPath = m_blockPathMap.get(id);
            try (SeekableByteChannel channel = Files.newByteChannel(blockPath)) {
                channel.read(block);
            }
            finally {
                block.position(origPosition);
            }
        }
    }

    /**
     * The block with the given ID is no longer needed, so delete it from disk.
     * @param id   The ID of the block to release
     * @throws IOException
     */
    public void releaseBlock(long id) throws IOException {
        synchronized (m_accessLock) {
            if (! m_blockPathMap.containsKey(id)) {
                throw new IllegalArgumentException("Request to release block that is not stored: " + id);
            }

            Path blockPath = m_blockPathMap.get(id);
            Files.delete(blockPath);
            m_blockPathMap.remove(id);
        }
    }

    /**
     * Release all the blocks that are on disk, and delete them from the
     * map that tracks them.
     * @throws IOException
     */
    public void releaseAllBlocks() throws IOException {
        synchronized (m_accessLock) {
            Set<Map.Entry<Long, Path>> entries = m_blockPathMap.entrySet();
            while (! entries.isEmpty()) {
                Map.Entry<Long, Path> entry = entries.iterator().next();
                Files.delete(entry.getValue());
                m_blockPathMap.remove(entry.getKey());
                entries = m_blockPathMap.entrySet();
            }
        }
    }

    // Given an ID, generate the Path for it.
    // It would be weird to have a file name starting with a "-",
    // so format the ID as unsigned.
    // Given package visibility for unit testing purposes.
    Path makeBlockPath(long id) {
        final BigInteger BIT_64 = BigInteger.ONE.shiftLeft(64);

        BigInteger b = BigInteger.valueOf(id);
        if(b.signum() < 0) {
            b = b.add(BIT_64);
        }

        String filename = b.toString() + ".block";
        return m_largeQuerySwapPath.resolve(filename);
    }
}
