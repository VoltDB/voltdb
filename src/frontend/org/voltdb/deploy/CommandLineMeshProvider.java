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
import static org.voltdb.VoltDB.DEFAULT_HTTP_PORT;

import java.util.NavigableSet;
import java.util.UUID;

import org.voltdb.utils.Digester;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;

public class CommandLineMeshProvider {

    protected final MemberNetConfig own;
    protected final ImmutableSortedSet<String> members;
    protected final UUID meshHash;

    public CommandLineMeshProvider(MemberNetConfig own, String option) {
        checkArgument(own != null, "own member config is null");
        this.own = own;

        checkArgument(option != null && !option.trim().isEmpty(),"option is null, empty or blank");
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        ImmutableSortedSet.Builder<String> sbld = ImmutableSortedSet.naturalOrder();
        for (String h: commaSplitter.split(option)) {
            checkArgument(MiscUtils.isValidHttpHostSpec(h), "%s is not a valid host spec");
            sbld.add(HostAndPort.fromString(h).withDefaultPort(DEFAULT_HTTP_PORT).toString());
        }
        this.members = sbld.build();
        checkArgument(!members.isEmpty(), "mesh contains no members");
        this.meshHash = Digester.md5AsUUID(own.getClusterName() + '|' + members.toString());
    }

    public MemberNetConfig getOwnConfig(String member) {
        return own;
    }

    public UUID getMeshHash() {
        return meshHash;
    }

    public NavigableSet<String> getMeshMembers() {
        return members;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((members == null) ? 0 : members.hashCode());
        result = prime * result
                + ((meshHash == null) ? 0 : meshHash.hashCode());
        result = prime * result + ((own == null) ? 0 : own.hashCode());
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
        CommandLineMeshProvider other = (CommandLineMeshProvider) obj;
        if (members == null) {
            if (other.members != null)
                return false;
        } else if (!members.equals(other.members))
            return false;
        if (meshHash == null) {
            if (other.meshHash != null)
                return false;
        } else if (!meshHash.equals(other.meshHash))
            return false;
        if (own == null) {
            if (other.own != null)
                return false;
        } else if (!own.equals(other.own))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CommandLineMeshProvider [own=");
        builder.append(own);
        builder.append(", members=");
        builder.append(members);
        builder.append(", meshHash=");
        builder.append(meshHash);
        builder.append("]");
        return builder.toString();
    }
}
