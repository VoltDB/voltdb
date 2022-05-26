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
package org.voltdb.largequery;

import java.io.File;
import java.io.IOException;
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
import java.util.concurrent.Future;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.io.FileUtils;
import org.voltcore.utils.CoreUtils;

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
    private static LargeBlockManager INSTANCE = null;

    private final static Set<OpenOption> OPEN_OPTIONS = new HashSet<>();
    private final static FileAttribute<Set<PosixFilePermission>> PERMISSIONS;

    private final Path m_largeQuerySwapPath;
    private final Map<BlockId, Path> m_blockPathMap = new HashMap<>();
    private final Object m_accessLock = new Object();
    private final ListeningExecutorService m_es = CoreUtils.getCachedSingleThreadExecutor("LargeBlockManager", 1000);

    static {
        OPEN_OPTIONS.add(StandardOpenOption.CREATE_NEW);
        OPEN_OPTIONS.add(StandardOpenOption.WRITE);
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
        PERMISSIONS = PosixFilePermissions.asFileAttribute(perms);
    }

    /**
     * This method creates the instance of LargeBlockManager (if it does not exist) and
     * clears out any files in the large query swap directory.
     * @param largeQuerySwapPath   Path to the directory (specified in deployment) where
     *   large query blocks are stored
     * @throws IOException if for some reason we cannot delete files
     */
    public static void startup(Path largeQuerySwapPath) throws IOException {

        // There could be an old instance hanging around in the case of some
        // JUnit tests that have an in-process server that is re-used.  This is
        // okay.  Create a new instance of LargeBlockManager regardless.

        INSTANCE = new LargeBlockManager(largeQuerySwapPath);
        INSTANCE.startupInstance();
    }

    /**
     * This method is called as the database is shutting down.
     * It releases any stored blocks and clears the swap directory.
     * @throws IOException
     */
    public static void shutdown() throws IOException {
        if (INSTANCE != null) {
            INSTANCE.shutdownInstance();
        }
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
     * On startup, clear out the large query swap directory.
     * @throws IOException
     */
    private void startupInstance() throws IOException {
        assert (m_blockPathMap.isEmpty());
        try {
            clearSwapDir();
        }
        catch (Exception e) {
            throw new IOException("Unable to clear large query swap directory: " + e.getMessage());
        }
    }

    /**
     * On shutdown, clear the map that tracks large query blocks,
     * and clear out the directory of blocks on disk.
     * @throws IOException
     */
    private void shutdownInstance() throws IOException {
        releaseAllBlocks();
        try {
            clearSwapDir();
        }
        catch (Exception e) {
            throw new IOException("Unable to clear large query swap directory: " + e.getMessage());
        }
    }

    /**
     * Cleans out the large_query_swap directory of all files.
     * Large query intermediate storage and results do not persist
     * across shutdown/startup.
     * This method is to clean up any spurious files that may not have
     * been deleted due to an unexpected shutdown.
     * @throws IOException
     */
    private void clearSwapDir() throws IOException {
        if (! m_blockPathMap.isEmpty()) {
            throw new IllegalStateException("Attempt to clear swap directory when "
                    + "there are still managed blocks; use releaseAllBlocks() instead");
        }

        FileUtils.cleanDirectory(m_largeQuerySwapPath.toFile());
   }

    public Future<LargeBlockResponse> submitTask(LargeBlockTask task) {
        return m_es.submit(task);
    }

    /**
     * Store the given block with the given ID to disk.
     * @param blockId      the ID of the block
     * @param block        the bytes for the block
     * @throws IOException
     */
    void storeBlock(BlockId blockId, ByteBuffer block) throws IOException {
        synchronized (m_accessLock) {
            if (m_blockPathMap.containsKey(blockId)) {
                throw new IllegalArgumentException("Request to store block that is already stored: "
                                                    + blockId.toString());
            }

            int origPosition = block.position();
            block.position(0);
            Path blockPath = makeBlockPath(blockId);
            try (SeekableByteChannel channel = Files.newByteChannel(blockPath, OPEN_OPTIONS, PERMISSIONS)) {
                channel.write(block);
            }
            finally {
                block.position(origPosition);
            }

            m_blockPathMap.put(blockId, blockPath);
        }
    }

    /**
     * Read the block with the given ID into the given byte buffer.
     * @param blockId  block id of the block to load
     * @param block    The block to write the bytes to
     * @return The original address of the block
     * @throws IOException
     */
    void loadBlock(BlockId blockId, ByteBuffer block) throws IOException {
        synchronized (m_accessLock) {
            if (! m_blockPathMap.containsKey(blockId)) {
                throw new IllegalArgumentException("Request to load block that is not stored: " + blockId);
            }

            int origPosition = block.position();
            block.position(0);
            Path blockPath = m_blockPathMap.get(blockId);
            try (SeekableByteChannel channel = Files.newByteChannel(blockPath)) {
                channel.read(block);
            }
            finally {
                block.position(origPosition);
            }
        }
    }

    /**
     * The block with the given site id and block counter is no longer needed, so delete it from disk.
     * @param blockId        The blockId of the block to release.
     * @throws IOException
     */
    void releaseBlock(BlockId blockId) throws IOException {
        synchronized (m_accessLock) {
            if (! m_blockPathMap.containsKey(blockId)) {
                throw new IllegalArgumentException("Request to release block that is not stored: " + blockId);
            }

            Path blockPath = m_blockPathMap.get(blockId);
            Files.delete(blockPath);
            m_blockPathMap.remove(blockId);
        }
    }

    /**
     * Release all the blocks that are on disk, and delete them from the
     * map that tracks them.
     * @throws IOException
     */
    private void releaseAllBlocks() throws IOException {
        synchronized (m_accessLock) {
            Set<Map.Entry<BlockId, Path>> entries = m_blockPathMap.entrySet();
            while (! entries.isEmpty()) {
                Map.Entry<BlockId, Path> entry = entries.iterator().next();
                Files.delete(entry.getValue());
                m_blockPathMap.remove(entry.getKey());
                entries = m_blockPathMap.entrySet();
            }
        }
    }

    // Given an ID, generate the Path for it.
    // Given package visibility for unit testing purposes.
    Path makeBlockPath(BlockId id) {
        String filename = id.fileNameString();
        return m_largeQuerySwapPath.resolve(filename);
    }
}
