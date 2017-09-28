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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class LargeBlockManager {
    private final Path m_lttSwapPath;
    private final Map<Long, Path> m_blockPathMap = new HashMap<>();

    public LargeBlockManager(Path lttSwapPath) {
        m_lttSwapPath = lttSwapPath;
    }

    public void storeBlock(long id, ByteBuffer block) {
        if (m_blockPathMap.containsKey(id)) {
            throw new IllegalArgumentException("Request to store block that is already stored: " + id);
        }

        Path blockPath = makeBlockPath(id);
        // Write block to disk...
        m_blockPathMap.put(id, blockPath);
    }

    public ByteBuffer loadBlock(long id) {
        if (! m_blockPathMap.containsKey(id)) {
            throw new IllegalArgumentException("Request to load block that is not stored: " + id);
        }

        // read from disk and return data in a ByteBuffer
        return null;
    }

    public void releaseBlock(long id) {
        if (! m_blockPathMap.containsKey(id)) {
            throw new IllegalArgumentException("Request to release block that is not stored: " + id);
        }

        // Delete file from disk, update map.

        m_blockPathMap.remove(id);
    }

    private Path makeBlockPath(long id) {
        String filename = Long.toString(id) + ".lttblock";
        return m_lttSwapPath.resolve(filename);
    }
}
