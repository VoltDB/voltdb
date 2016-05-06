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

import org.codehaus.jackson.annotate.JsonIgnore;

public class KSafetyResponse {
    /**
     * Not yet available
     */
    public final static int NYA = -1;

    protected final UUID startUuid;
    protected final int lack;
    protected final int hosts;

    public KSafetyResponse(UUID startUuid, int lack, int hosts) {
        checkArgument(startUuid != null, "start uuid is null");
        checkArgument(lack == NYA || lack >=0, "invalid lack value %s", lack);
        checkArgument(hosts == NYA || lack > 0, "invalid hosts value %s", hosts);
        this.startUuid = startUuid;
        this.lack = lack;
        this.hosts = hosts;
    }

    public KSafetyResponse() {
        this(new UUID(0L,0L), NYA, NYA);
    }

    public UUID getStartUuid() {
        return startUuid;
    }

    public int getLack() {
        return lack;
    }

    public int getHosts() {
        return hosts;
    }

    @JsonIgnore
    public boolean isNotYetAvailable() {
        return lack == NYA;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + hosts;
        result = prime * result + lack;
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
        KSafetyResponse other = (KSafetyResponse) obj;
        if (hosts != other.hosts)
            return false;
        if (lack != other.lack)
            return false;
        if (startUuid == null) {
            if (other.startUuid != null)
                return false;
        } else if (!startUuid.equals(other.startUuid))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "KSafetyResponse [startUuid=" + startUuid + ", lack=" + lack
                + ", hosts=" + hosts + "]";
    }
}
