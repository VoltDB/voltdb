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

package org.voltcore.messaging;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static org.voltdb.VoltDB.DEFAULT_INTERNAL_PORT;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.UUID;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.StartAction;
import org.voltdb.VersionChecker;
import org.voltdb.common.NodeState;
import org.voltdb.utils.Digester;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.net.InetAddresses;
import com.google_voltpatches.common.net.InternetDomainName;

public class JoinerCriteria {

    public static ImmutableSortedSet<String> hosts(String option) {
        checkArgument(option != null, "option is null");
        if (option.trim().isEmpty()) {
            return ImmutableSortedSet.of(
                    HostAndPort.fromParts("", DEFAULT_INTERNAL_PORT).toString());
        }
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        ImmutableSortedSet.Builder<String> sbld = ImmutableSortedSet.naturalOrder();
        for (String h: commaSplitter.split(option)) {
            checkArgument(isValidCoordinatorSpec(h), "%s is not a valid host spec", h);
            sbld.add(HostAndPort.fromString(h).withDefaultPort(DEFAULT_INTERNAL_PORT).toString());
        }
        return sbld.build();
    }

    public static ImmutableSortedSet<String> hosts(int...ports) {
        if (ports.length == 0) {
            return ImmutableSortedSet.of(
                    HostAndPort.fromParts("", DEFAULT_INTERNAL_PORT).toString());
        }
        ImmutableSortedSet.Builder<String> sbld = ImmutableSortedSet.naturalOrder();
        for (int p: ports) {
            sbld.add(HostAndPort.fromParts("", p).toString());
        }
        return sbld.build();
    }

    public static boolean isValidCoordinatorSpec(String specifier) {
        if (specifier == null) {
            return false;
        }

        if (specifier.isEmpty()) {
            return true;
        }

        final HostAndPort parsedHost = HostAndPort
                .fromString(specifier)
                .withDefaultPort(DEFAULT_INTERNAL_PORT);
        final String host = parsedHost.getHostText();
        if (host.isEmpty()) {
            return true;
        }

        // Try to interpret the specifier as an IP address.  Note we build
        // the address rather than using the .is* methods because we want to
        // use InetAddresses.toUriString to convert the result to a string in
        // canonical form.
        InetAddress addr = null;
        try {
          addr = InetAddresses.forString(host);
        } catch (IllegalArgumentException e) {
          // It is not an IPv4 or IPv6 literal
        }

        if (addr != null) {
          return true;
        }
        // It is not any kind of IP address; must be a domain name or invalid.
        return InternetDomainName.isValid(host);
    }

    protected final ImmutableSortedSet<String> m_coordinators;
    protected final VersionChecker m_versionChecker;
    protected final boolean m_enterprise;
    protected final StartAction m_startAction;
    protected final boolean m_bare;
    protected final UUID m_configHash;
    protected final UUID m_meshHash;
    protected final int m_hostCount;
    protected final int m_kFactor;
    protected final boolean m_paused;
    protected final Supplier<NodeState> m_nodeStateSupplier;
    protected final boolean m_addAllowed;

    protected JoinerCriteria(NavigableSet<String> coordinators, VersionChecker versionChecker,
            boolean enterprise, StartAction startAction, boolean bare,
            UUID configHash,int hostCount, int kFactor, boolean paused,
            Supplier<NodeState> nodeStateSupplier, boolean addAllowed) {

        checkArgument(versionChecker != null, "version checker is null");
        checkArgument(configHash != null, "config hash is null");
        checkArgument(startAction != null, "start action is null");
        checkArgument(nodeStateSupplier != null, "nodeStateSupplier is null");
        checkArgument(hostCount > 0, "invalid host count value: %s",hostCount);
        checkArgument(kFactor >= 0, "invalid kFactor value: %s", kFactor);
        checkArgument(coordinators != null &&
                coordinators.stream().allMatch(h->isValidCoordinatorSpec(h)),
                "coordinators is null or contains invalid host/interface specs %s", coordinators);

        this.m_coordinators = ImmutableSortedSet.copyOf(coordinators);
        this.m_versionChecker = versionChecker;
        this.m_enterprise = enterprise;
        this.m_startAction = startAction;
        this.m_bare = bare;
        this.m_configHash = configHash;
        this.m_hostCount = hostCount;
        this.m_kFactor = kFactor;
        this.m_paused = paused;
        this.m_nodeStateSupplier = nodeStateSupplier;
        this.m_addAllowed = addAllowed;

        this.m_meshHash = Digester.md5AsUUID("hostCount="+ hostCount + '|' + this.m_coordinators.toString());
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

    public VersionChecker getVersionChecker() {
        return m_versionChecker;
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

    public boolean isPaused() {
        return m_paused;
    }

    public boolean isAddAllowed() {
        return m_addAllowed;
    }

    public HostCriteria asHostCriteria() {
        return new HostCriteria(
                m_paused,
                m_configHash,
                m_meshHash,
                m_enterprise,
                m_startAction,
                m_bare,
                m_hostCount,
                m_nodeStateSupplier.get(),
                m_addAllowed
                );
    }

    public HostCriteria asHostCriteria(boolean adminMode) {
        return new HostCriteria(
                adminMode,
                m_configHash,
                m_meshHash,
                m_enterprise,
                m_startAction,
                m_bare,
                m_hostCount,
                m_nodeStateSupplier.get(),
                m_addAllowed
                );
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (m_paused ? 1231 : 1237);
        result = prime * result + (m_bare ? 1231 : 1237);
        result = prime * result + (m_addAllowed ? 1231 : 1237);
        result = prime * result
                + ((m_configHash == null) ? 0 : m_configHash.hashCode());
        result = prime * result + (m_enterprise ? 1231 : 1237);
        result = prime * result + m_hostCount;
        result = prime * result + m_kFactor;
        result = prime * result + ((m_coordinators == null) ? 0 : m_coordinators.hashCode());
        result = prime * result
                + ((m_meshHash == null) ? 0 : m_meshHash.hashCode());
        result = prime * result
                + ((m_startAction == null) ? 0 : m_startAction.hashCode());
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
        JoinerCriteria other = (JoinerCriteria) obj;
        if (m_paused != other.m_paused)
            return false;
        if (m_bare != other.m_bare)
            return false;
        if (m_addAllowed != other.m_addAllowed)
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
        if (m_startAction != other.m_startAction)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "JoinerCriteria [coordinators=" + m_coordinators
                + ", enterprise=" + m_enterprise + ", startAction=" + m_startAction
                + ", bare=" + m_bare + ", configHash=" + m_configHash
                + ", meshHash=" + m_meshHash + ", hostCount=" + m_hostCount
                + ", kFactor=" + m_kFactor + ", paused=" + m_paused
                + ", addAllowed=" + m_addAllowed + "]";
    }

    public void appendTo(JSONStringer js) throws JSONException {
        js.object();
        js.key("coordinators").array();
        for (String coordinator: m_coordinators) {
            js.value(coordinator);
        }
        js.endArray();
        js.key("enterprise").value(m_enterprise);
        js.key("startAction").value(m_startAction.name());
        js.key("bare").value(m_bare);
        js.key("configHash").value(m_configHash.toString());
        js.key("meshHash").value(m_meshHash.toString());
        js.key("hostCount").value(m_hostCount);
        js.key("kFactor").value(m_kFactor);
        js.key("paused").value(m_paused);
        js.key("addAllowed").value(m_addAllowed);
        js.endObject();
    }

    public String asJSON() {
        JSONStringer js = new JSONStringer();
        try {
            appendTo(js);
        } catch (JSONException e) {
            Throwables.propagate(e);
        }
        return js.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        static final protected Supplier<String[]> versionSupplier =
                Suppliers.memoize(
                        new Supplier<String[]>() {
                            @Override
                            public String[] get() {
                                return org.voltdb.RealVoltDB.extractBuildInfo(null);
                            }
                        });

        protected final VersionChecker m_defaultVersionChecker = new VersionChecker() {
            @Override
            public boolean isCompatibleVersionString(String other) {
                return org.voltdb.RealVoltDB.staticIsCompatibleVersionString(other);
            }
            @Override
            public String getVersionString() {
                return versionSupplier.get()[0];
            }
            @Override
            public String getBuildString() {
                return versionSupplier.get()[1];
            }
        };

        protected NavigableSet<String> m_coordinators = ImmutableSortedSet.of("localhost");
        protected VersionChecker m_versionChecker = m_defaultVersionChecker;
        protected boolean m_enterprise = MiscUtils.isPro();
        protected StartAction m_startAction = StartAction.PROBE;
        protected boolean m_bare = true;
        protected UUID m_configHash = new UUID(0L, 0L);
        protected int m_hostCount = 1;
        protected int m_kFactor = 0;
        protected boolean m_paused = false;
        protected Supplier<NodeState> m_nodeStateSupplier =
                Suppliers.ofInstance(NodeState.INITIALIZING);
        protected boolean m_addAllowed = false;

        protected Builder() {
        }

        public Builder versionChecker(VersionChecker versionChecker) {
            m_versionChecker = checkNotNull(versionChecker);
            return this;
        }

        public Builder startAction(StartAction startAction) {
            m_startAction = checkNotNull(startAction);
            return this;
        }

        public Builder nodeState(NodeState nodeState) {
            m_nodeStateSupplier = Suppliers.ofInstance(checkNotNull(nodeState));
            return this;
        }

        public Builder configHash(UUID configHash) {
            m_configHash = checkNotNull(configHash);
            return this;
        }

        public Builder coordinators(NavigableSet<String> coordinators) {
            m_coordinators = checkNotNull(coordinators);
            return this;
        }

        public Builder coordinators(String...hosts) {
            checkArgument(hosts.length > 0, "no hosts provided");
            checkArgument(Arrays.stream(hosts).allMatch(h->isValidCoordinatorSpec(h)),
                    "coordinators contains invalid host/interface specs %s", Arrays.toString(hosts));
            m_coordinators = ImmutableSortedSet.copyOf(hosts);
            return this;
        }

        public Builder bare(boolean bare) {
            m_bare = bare;
            return this;
        }

        public Builder enterprise(boolean enterprise) {
            m_enterprise = enterprise;
            return this;
        }

        public Builder paused(boolean paused) {
            m_paused = paused;
            return this;
        }

        public Builder kfactor(int kfactor) {
            m_kFactor = kfactor;
            return this;
        }

        public Builder hostCount(int hostCount) {
            m_hostCount = hostCount;
            return this;
        }

        public Builder nodeStateSupplier(Supplier<NodeState> supplier) {
            m_nodeStateSupplier = supplier;
            return this;
        }

        public Builder addAllowed(boolean addAllowed) {
            m_addAllowed = addAllowed;
            return this;
        }

        public JoinerCriteria build() {
            return new JoinerCriteria(
                    m_coordinators,
                    m_versionChecker,
                    m_enterprise,
                    m_startAction,
                    m_bare,
                    m_configHash,
                    m_hostCount < m_coordinators.size() ? m_coordinators.size() : m_hostCount,
                    m_kFactor,
                    m_paused,
                    m_nodeStateSupplier,
                    m_addAllowed
                    );
        }
    }
 }
