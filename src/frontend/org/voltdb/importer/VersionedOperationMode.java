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

package org.voltdb.importer;

import java.util.concurrent.atomic.AtomicStampedReference;

import org.voltdb.OperationMode;

public final class VersionedOperationMode {
    final OperationMode mode;
    final int version;

    VersionedOperationMode(AtomicStampedReference<OperationMode> ref) {
        if (ref == null) {
            throw new IllegalArgumentException("stamped reference is null");
        }
        int [] stamp = new int[]{0};
        this.mode = ref.get(stamp);
        this.version = stamp[0];
    }

    VersionedOperationMode(OperationMode mode, int version) {
        if (mode == null) {
            throw new IllegalArgumentException("operation mode is null");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version is less than 0");
        }
        this.mode = mode;
        this.version = version;
    }

    public OperationMode getMode() {
        return mode;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((mode == null) ? 0 : mode.hashCode());
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VersionedOperationMode other = (VersionedOperationMode) obj;
        if (mode != other.mode)
            return false;
        if (version != other.version)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "VersionedOperationalMode [mode=" + mode + ", version="
                + version + "]";
    }
}
