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

package org.voltdb;

import java.nio.ByteBuffer;

public abstract class DependencyPair {

    public final int depId;

    public DependencyPair(int depId) {
        this.depId = depId;
    }

    public abstract ByteBuffer getBufferDependency();

    public abstract VoltTable getTableDependency();

    public static class TableDependencyPair extends DependencyPair {
        private final VoltTable dependencyTable;

        public TableDependencyPair(int depId, VoltTable dependency) {
            super(depId);
            assert(dependency != null);

            this.dependencyTable = dependency;
        }

        public ByteBuffer getBufferDependency() {
            if (dependencyTable == null) {
                return null;
            }
            return TableHelper.getBackedBuffer(dependencyTable);
        }

        public VoltTable getTableDependency() {
            return dependencyTable;
        }
    }

    public static class BufferDependencyPair extends DependencyPair {
        private final byte[] dependencyByteArray;
        private final int startPosition;
        private final int totalLen;
        private VoltTable dependencyTable = null;

        public BufferDependencyPair(int depId, byte[] dependency, int startPosition, int totalLen) {
            super(depId);
            assert(dependency != null);
            assert(dependency.length >= 4);
            this.dependencyByteArray = dependency;
            this.startPosition = startPosition;
            this.totalLen = totalLen;
        }


        public ByteBuffer getBufferDependency() {
            return ByteBuffer.wrap(dependencyByteArray, startPosition, totalLen);
        }

        private static int byteArrayToInt(byte[] b, int position)
        {
            return   b[position+3] & 0xFF |
                    (b[position+2] & 0xFF) << 8 |
                    (b[position+1] & 0xFF) << 16 |
                    (b[position] & 0xFF) << 24;
        }

        public VoltTable getTableDependency() {
            if (dependencyTable == null) {
                dependencyTable = PrivateVoltTableFactory.createVoltTableFromByteArray(dependencyByteArray, startPosition, totalLen);
            }
            return dependencyTable;
        }
    }
}


