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
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A class that manages large blocks produced by large queries
 */
public class LargeBlockManager {
    private final Path m_lttSwapPath;
    private final Map<Long, Path> m_blockPathMap = new HashMap<>();

    private final static Set<OpenOption> OPEN_OPTIONS = new HashSet<>();
    private final static FileAttribute<Set<PosixFilePermission>> PERMISSIONS;

    static {
        OPEN_OPTIONS.add(StandardOpenOption.CREATE_NEW);
        OPEN_OPTIONS.add(StandardOpenOption.WRITE);
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
        PERMISSIONS = PosixFilePermissions.asFileAttribute(perms);
    }

    public LargeBlockManager(Path lttSwapPath) {
        m_lttSwapPath = lttSwapPath;
    }

    public void storeBlock(long id, ByteBuffer block) throws IOException {
        synchronized (this) {
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

    public void loadBlock(long id, ByteBuffer block) throws IOException {
        synchronized (this) {
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

    public void releaseBlock(long id) throws IOException {
        synchronized (this) {
            if (! m_blockPathMap.containsKey(id)) {
                throw new IllegalArgumentException("Request to release block that is not stored: " + id);
            }

            Path blockPath = m_blockPathMap.get(id);
            Files.delete(blockPath);
            m_blockPathMap.remove(id);
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

        String filename = b.toString() + ".lttblock";
        return m_lttSwapPath.resolve(filename);
    }
}
