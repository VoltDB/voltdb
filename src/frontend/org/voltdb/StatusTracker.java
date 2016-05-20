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
package org.voltdb;

import java.util.UUID;

import org.voltdb.common.NodeState;

/**
 *
 */
public class StatusTracker {
    private volatile NodeState nodeState;
    private final UUID startUuid;
    private final String clusterName;
    private final String buildInfo;
    private final String versionInfo;
    private final boolean enterprise;

    public StatusTracker(NodeState nodeState, UUID startUuid,
            String clusterName, String buildInfo, String versionInfo,
            boolean enterprise) {
        this.nodeState = nodeState;
        this.startUuid = startUuid;
        this.clusterName = clusterName;
        this.buildInfo = buildInfo;
        this.versionInfo = versionInfo;
        this.enterprise = enterprise;
    }

    public StatusTracker(UUID startUuid, String clusterName,
            String buildInfo, String versionInfo,
            boolean enterprise) {
        this(NodeState.INITIALIZING, startUuid, clusterName, buildInfo, versionInfo, enterprise);
    }

    public StatusTracker() {
       this(NodeState.INITIALIZING,new UUID(0L,0L),null,null,null,false);
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    public void setNodeState(NodeState nodeState) {
        this.nodeState = nodeState;
    }

    public UUID getStartUuid() {
        return startUuid;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBuildInfo() {
        return buildInfo;
    }

    public String getVersionInfo() {
        return versionInfo;
    }

    public boolean isEnterprise() {
        return enterprise;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((buildInfo == null) ? 0 : buildInfo.hashCode());
        result = prime * result
                + ((clusterName == null) ? 0 : clusterName.hashCode());
        result = prime * result + (enterprise ? 1231 : 1237);
        result = prime * result
                + ((nodeState == null) ? 0 : nodeState.hashCode());
        result = prime * result
                + ((startUuid == null) ? 0 : startUuid.hashCode());
        result = prime * result
                + ((versionInfo == null) ? 0 : versionInfo.hashCode());
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
        StatusTracker other = (StatusTracker) obj;
        if (buildInfo == null) {
            if (other.buildInfo != null)
                return false;
        } else if (!buildInfo.equals(other.buildInfo))
            return false;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        if (enterprise != other.enterprise)
            return false;
        if (nodeState != other.nodeState)
            return false;
        if (startUuid == null) {
            if (other.startUuid != null)
                return false;
        } else if (!startUuid.equals(other.startUuid))
            return false;
        if (versionInfo == null) {
            if (other.versionInfo != null)
                return false;
        } else if (!versionInfo.equals(other.versionInfo))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StatusTracker [nodeState=");
        builder.append(nodeState);
        builder.append(", startUuid=");
        builder.append(startUuid);
        builder.append(", buildInfo=");
        builder.append(buildInfo);
        builder.append(", versionInfo=");
        builder.append(versionInfo);
        builder.append(", isEnterprise=");
        builder.append(enterprise);
        builder.append(", clusterName=");
        builder.append(clusterName);
        builder.append("]");
        return builder.toString();
    }
}
