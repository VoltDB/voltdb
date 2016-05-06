/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.deploy;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.UUID;

public class LeaderAllClear {
    protected final UUID startUuid;

    public LeaderAllClear(UUID startUuid) {
        checkArgument(startUuid != null, "start uuid is null");
        this.startUuid = startUuid;
    }

    public LeaderAllClear() {
        this(new UUID(0L,0L));
    }

    public UUID getStartUuid() {
        return startUuid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((startUuid == null) ? 0 : startUuid.hashCode());
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
        LeaderAllClear other = (LeaderAllClear) obj;
        if (startUuid == null) {
            if (other.startUuid != null)
                return false;
        } else if (!startUuid.equals(other.startUuid))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "LeaderAllClear [startUuid=" + startUuid + "]";
    }
}
