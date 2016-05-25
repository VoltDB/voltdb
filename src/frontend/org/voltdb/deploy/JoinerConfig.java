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
import static org.voltdb.VoltDB.DEFAULT_INTERNAL_PORT;

import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Supplier;

import org.voltdb.StartAction;
import org.voltdb.common.NodeState;
import org.voltdb.utils.Digester;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;

public class JoinerConfig {

    public static ImmutableSortedSet<String> hosts(String option) {
        checkArgument(option != null && !option.trim().isEmpty(),"option is null, empty or blank");
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        ImmutableSortedSet.Builder<String> sbld = ImmutableSortedSet.naturalOrder();
        for (String h: commaSplitter.split(option)) {
            checkArgument(MiscUtils.isValidHostSpec(h, DEFAULT_INTERNAL_PORT), "%s is not a valid host spec", h);
            sbld.add(HostAndPort.fromString(h).withDefaultPort(DEFAULT_INTERNAL_PORT).toString());
        }
        return sbld.build();
    }

    protected final MemberNetConfig m_own;
    protected final ImmutableSortedSet<String> m_coordinators;
    protected final String m_buildInfo;
    protected final String m_versionInfo;
    protected final boolean m_enterprise;
    protected final StartAction m_startAction;
    protected final boolean m_bare;
    protected final UUID m_configHash;
    protected final UUID m_meshHash;
    protected final int m_hostCount;
    protected final int m_kFactor;
    protected final boolean m_adminMode;
    protected final Supplier<NodeState> m_nodeStateSupplier;

    public JoinerConfig(MemberNetConfig own, String coordinators,
            String buildInfo, String versionInfo, boolean enterprise,
            StartAction startAction, boolean bare, UUID configHash,
            int hostCount, int kFactor, boolean adminMode,
            Supplier<NodeState> nodeStateSupplier) {

        checkArgument(own != null, "own member config is null");
        checkArgument(buildInfo != null && !buildInfo.trim().isEmpty(), "buildInfo is null, empty, or blank");
        checkArgument(versionInfo != null && !versionInfo.trim().isEmpty(), "versionInfo is null, empty, or blank");
        checkArgument(configHash != null, "config hash is null");
        checkArgument(startAction != null, "start action is null");
        checkArgument(nodeStateSupplier != null, "nodeStateSupplier is null");
        checkArgument(hostCount > 1, "invalid host count value: %s",hostCount);
        checkArgument(kFactor >= 0, "invalid kFactor value: %s", kFactor);

        this.m_own = own;
        this.m_coordinators = hosts(coordinators);
        this.m_buildInfo = buildInfo;
        this.m_versionInfo = versionInfo;
        this.m_enterprise = enterprise;
        this.m_startAction = startAction;
        this.m_bare = bare;
        this.m_configHash = configHash;
        this.m_hostCount = hostCount;
        this.m_kFactor = kFactor;
        this.m_adminMode = adminMode;
        this.m_nodeStateSupplier = nodeStateSupplier;

        this.m_meshHash = Digester.md5AsUUID("hostCount="+ hostCount + '|' + this.m_coordinators.toString());
    }

    public MemberNetConfig getNetConfig() {
        return m_own;
    }

    public UUID getMeshHash() {
        return m_meshHash;
    }

    public NavigableSet<String> getCoordinators() {
        return m_coordinators;
    }

    public NodeState getNodeState() {
        return m_nodeStateSupplier.get();
    }

    public HostAndPort getLeader() {
        return HostAndPort.fromString(m_coordinators.first()).withDefaultPort(DEFAULT_INTERNAL_PORT);
    }

    public String getBuildInfo() {
        return m_buildInfo;
    }

    public String getVersionInfo() {
        return m_versionInfo;
    }

    public boolean isEnterprise() {
        return m_enterprise;
    }

    public StartAction getStartAction() {
        return m_startAction;
    }

    public boolean isBare() {
        return m_bare;
    }

    public UUID getConfigHash() {
        return m_configHash;
    }

    public int getHostCount() {
        return m_hostCount;
    }

    public int getkFactor() {
        return m_kFactor;
    }

    public boolean isAdminMode() {
        return m_adminMode;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (m_adminMode ? 1231 : 1237);
        result = prime * result + (m_bare ? 1231 : 1237);
        result = prime * result
                + ((m_buildInfo == null) ? 0 : m_buildInfo.hashCode());
        result = prime * result
                + ((m_configHash == null) ? 0 : m_configHash.hashCode());
        result = prime * result + (m_enterprise ? 1231 : 1237);
        result = prime * result + m_hostCount;
        result = prime * result + m_kFactor;
        result = prime * result + ((m_coordinators == null) ? 0 : m_coordinators.hashCode());
        result = prime * result
                + ((m_meshHash == null) ? 0 : m_meshHash.hashCode());
        result = prime * result + ((m_own == null) ? 0 : m_own.hashCode());
        result = prime * result
                + ((m_startAction == null) ? 0 : m_startAction.hashCode());
        result = prime * result
                + ((m_versionInfo == null) ? 0 : m_versionInfo.hashCode());
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
        JoinerConfig other = (JoinerConfig) obj;
        if (m_adminMode != other.m_adminMode)
            return false;
        if (m_bare != other.m_bare)
            return false;
        if (m_buildInfo == null) {
            if (other.m_buildInfo != null)
                return false;
        } else if (!m_buildInfo.equals(other.m_buildInfo))
            return false;
        if (m_configHash == null) {
            if (other.m_configHash != null)
                return false;
        } else if (!m_configHash.equals(other.m_configHash))
            return false;
        if (m_enterprise != other.m_enterprise)
            return false;
        if (m_hostCount != other.m_hostCount)
            return false;
        if (m_kFactor != other.m_kFactor)
            return false;
        if (m_coordinators == null) {
            if (other.m_coordinators != null)
                return false;
        } else if (!m_coordinators.equals(other.m_coordinators))
            return false;
        if (m_meshHash == null) {
            if (other.m_meshHash != null)
                return false;
        } else if (!m_meshHash.equals(other.m_meshHash))
            return false;
        if (m_own == null) {
            if (other.m_own != null)
                return false;
        } else if (!m_own.equals(other.m_own))
            return false;
        if (m_startAction != other.m_startAction)
            return false;
        if (m_versionInfo == null) {
            if (other.m_versionInfo != null)
                return false;
        } else if (!m_versionInfo.equals(other.m_versionInfo))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "JoinerConfig [own=" + m_own + ", coordinators=" + m_coordinators
                + ", buildInfo=" + m_buildInfo + ", versionInfo=" + m_versionInfo
                + ", enterprise=" + m_enterprise + ", startAction=" + m_startAction
                + ", bare=" + m_bare + ", configHash=" + m_configHash
                + ", meshHash=" + m_meshHash + ", hostCount=" + m_hostCount
                + ", kFactor=" + m_kFactor + ", adminMode=" + m_adminMode + "]";
    }
 }
