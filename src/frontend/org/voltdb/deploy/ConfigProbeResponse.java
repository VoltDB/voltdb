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
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static org.voltdb.VoltDB.DEFAULT_INTERNAL_PORT;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.annotate.JsonIgnore;

public class ConfigProbeResponse {

    public final static ConfigProbeResponse POISON_PILL;
    public final static Pattern portRE = Pattern.compile("(?:[0-9a-z._-]+)?:\\d+",Pattern.CASE_INSENSITIVE);
    public final static String DEFAULT_INTFC = ":" + Integer.toString(DEFAULT_INTERNAL_PORT);

    static {
        UUID poison = UUID.randomUUID();
        POISON_PILL = new ConfigProbeResponse(poison, poison, poison, DEFAULT_INTFC, false);
    }

    protected final UUID configHash;
    protected final UUID meshHash;
    protected final UUID startUuid;
    protected final String internalInterface;
    protected final boolean admin;

    public ConfigProbeResponse() {
        this(POISON_PILL.configHash,POISON_PILL.meshHash,POISON_PILL.startUuid,DEFAULT_INTFC,false);
    }

    public ConfigProbeResponse(UUID configHash, UUID meshHash, UUID startUuid, String internalInterface, boolean admin) {
        this.configHash = checkNotNull(configHash, "config has is null");
        this.meshHash = checkNotNull(meshHash, "config has is null");
        this.startUuid = checkNotNull(startUuid, "config has is null");
        checkArgument(internalInterface != null && !internalInterface.trim().isEmpty(),
                "internal interface is null, empty or blank");
        Matcher mtc = portRE.matcher(internalInterface);
        checkArgument(mtc.matches(), "invalid internal interface specification: %s", internalInterface);
        this.internalInterface = internalInterface;
        this.admin = admin;
    }

    public UUID getConfigHash() {
        return configHash;
    }

    public UUID getMeshHash() {
        return meshHash;
    }

    public UUID getStartUuid() {
        return startUuid;
    }

    public String getInternalInterface() {
        return internalInterface;
    }

    public boolean isAdmin() {
        return admin;
    }

    @JsonIgnore
    public boolean isPoison() {
        return configHash.equals(meshHash) && configHash.equals(startUuid);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (admin ? 1231 : 1237);
        result = prime * result
                + ((configHash == null) ? 0 : configHash.hashCode());
        result = prime * result + ((internalInterface == null) ? 0
                : internalInterface.hashCode());
        result = prime * result
                + ((meshHash == null) ? 0 : meshHash.hashCode());
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
        ConfigProbeResponse other = (ConfigProbeResponse) obj;
        if (admin != other.admin)
            return false;
        if (configHash == null) {
            if (other.configHash != null)
                return false;
        } else if (!configHash.equals(other.configHash))
            return false;
        if (internalInterface == null) {
            if (other.internalInterface != null)
                return false;
        } else if (!internalInterface.equals(other.internalInterface))
            return false;
        if (meshHash == null) {
            if (other.meshHash != null)
                return false;
        } else if (!meshHash.equals(other.meshHash))
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
        StringBuilder builder = new StringBuilder();
        builder.append("ConfigProbeResponse [configHash=");
        builder.append(configHash);
        builder.append(", meshHash=");
        builder.append(meshHash);
        builder.append(", startUuid=");
        builder.append(startUuid);
        builder.append(", internalInterface=");
        builder.append(internalInterface);
        builder.append(", admin=");
        builder.append(admin);
        builder.append("]");
        return builder.toString();
    }
}
