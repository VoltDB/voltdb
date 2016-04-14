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

import org.voltdb.common.NodeState;

/**
 *
 * @author akhanzode
 */
public class StatusTracker implements StatusProvider {
    private NodeState m_nodeState = NodeState.INITIALIZING;
    private final String m_buildInfo;
    private final String m_versionInfo;
    private final boolean m_isEnterprise;
    private final String m_clusterName;

    public StatusTracker(String clusterName, String buildInfo, String versionInfo, boolean enterprise) {
        m_clusterName = clusterName;
        m_buildInfo = buildInfo;
        m_versionInfo = versionInfo;
        m_isEnterprise = enterprise;
    }

    @Override
    public String getVersion() {
        return m_versionInfo;
    }

    @Override
    public String getBuild() {
        return m_buildInfo;
    }

    @Override
    public boolean isEnterprise() {
        return m_isEnterprise;
    }

    @Override
    public String getClusterName() {
        return m_clusterName;
    }

    @Override
    public NodeState getNodeState() {
        return m_nodeState;
    }

    @Override
    public void setNodeState(NodeState s) {
        m_nodeState = s;
    }

}
