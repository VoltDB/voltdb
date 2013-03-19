/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltdb;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;

public class LegacyHashinator extends TheHashinator {
    private final int catalogPartitionCount;
    private final byte m_configBytes[];
    private static final VoltLogger hostLogger = new VoltLogger("HOST");

    @Override
    protected int pHashinateBytes(byte[] bytes) {
        int hashCode = 0;
        int offset = 0;
        for (int ii = 0; ii < bytes.length; ii++) {
            hashCode = 31 * hashCode + bytes[offset++];
        }
        return java.lang.Math.abs(hashCode % catalogPartitionCount);
    }

    public LegacyHashinator(byte config[]) {
        catalogPartitionCount = ByteBuffer.wrap(config).getInt();
        m_configBytes = Arrays.copyOf(config, config.length);
    }

    public static byte[] getConfigureBytes(int catalogPartitionCount) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(catalogPartitionCount);
        return buf.array();
    }

    @Override
    protected Pair<HashinatorType, byte[]> pGetCurrentConfig() {
        return Pair.of(HashinatorType.LEGACY, m_configBytes);
    }
}
