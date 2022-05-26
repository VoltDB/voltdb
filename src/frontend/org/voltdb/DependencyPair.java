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

package org.voltdb;

import java.nio.ByteBuffer;

/*
 * DependencyPairs represent a relationship between the steps of an MP System Procedure we are
 * currently on and the Table result that is associated with that step. In some cases these dependencies
 * are passed around between processes on the same host in which case they are actual Tables, but in
 * other cases they are received over the network, in which case they are serialized into arrays.
 * It is often convenient to leave them in whatever form they were created in and defer the conversion
 * until it is actually necessary.
 */
public abstract class DependencyPair {

    public final int depId;

    public DependencyPair(int depId) {
        this.depId = depId;
    }

    public abstract ByteBuffer getBufferDependency();

    public abstract VoltTable getTableDependency();

    /*
     * Concrete class for a DependencyPair that is created from a VoltTable but may
     * need to be serialized into a ByteArray.
     */
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

    /*
     * Concrete class for a DependencyPair that is created from a ByteArray (typically
     * from the network) that may need to be represented as a VoltTable.
     */
    public static class BufferDependencyPair extends DependencyPair {
        private final byte[] dependencyByteArray;
        private final int startPosition;
        private final int totalLen;
        private VoltTable dependencyTable = null;

        public BufferDependencyPair(int depId, byte[] dependency, int startPosition, int totalLen) {
            super(depId);
            assert (dependency != null);
            assert (dependency.length >= 4);
            this.dependencyByteArray = dependency;
            this.startPosition = startPosition;
            this.totalLen = totalLen;
        }

        public ByteBuffer getBufferDependency() {
            return ByteBuffer.wrap(dependencyByteArray, startPosition, totalLen);
        }

        public VoltTable getTableDependency() {
            if (dependencyTable == null) {
                dependencyTable = PrivateVoltTableFactory.createVoltTableFromByteArray(dependencyByteArray, startPosition, totalLen);
            }
            return dependencyTable;
        }
    }
}


