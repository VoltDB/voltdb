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

package org.voltdb.probe;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.not;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Generated;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.json_voltpatches.JSONWriter;
import org.voltcore.common.Constants;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.JoinAcceptor;
import org.voltcore.messaging.SocketJoiner;
import org.voltcore.utils.VersionChecker;
import org.voltcore.zk.CoreZK;
import org.voltdb.StartAction;
import org.voltdb.common.NodeState;
import org.voltdb.utils.Digester;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.net.InetAddresses;
import com.google_voltpatches.common.net.InternetDomainName;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 * The VoltDB implementation of {@link JoinAcceptor} that piggy backs the mesh
 * establishment messages to determine if connecting nodes are compatible, and
 * if they are, it determines the voltdb start action from the information
 * gathered from other connecting nodes.
 *
 */
public class MeshProber implements JoinAcceptor {

    private static final String COORDINATORS = "coordinators";
    private static final String SAFE_MODE = "safeMode";
    private static final String ADD_ALLOWED = "addAllowed";
    private static final String PAUSED = "paused";
    private static final String K_FACTOR = "kFactor";
    private static final String HOST_COUNT = "hostCount";
    private static final String MESH_HASH = "meshHash";
    private static final String CONFIG_HASH = "configHash";
    private static final String BARE = "bare";
    private static final String START_ACTION = "startAction";
    private static final String ENTERPRISE = "enterprise";
    private static final String TERMINUS_NONCE = "terminusNonce";

    private static final VoltLogger m_networkLog = new VoltLogger("NETWORK");

    /**
     * Helper method that takes a comma delimited list of host specs, validates it,
     * and converts it to a set of valid coordinators
     * @param option a string that contains comma delimited list of host specs
     * @return a set of valid coordinators
     */
    public static ImmutableSortedSet<String> hosts(String option) {
        checkArgument(option != null, "option is null");
        if (option.trim().isEmpty()) {
            return ImmutableSortedSet.of(
                    HostAndPort.fromParts("", Constants.DEFAULT_INTERNAL_PORT).toString());
        }
        Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
        ImmutableSortedSet.Builder<String> sbld = ImmutableSortedSet.naturalOrder();
        for (String h: commaSplitter.split(option)) {
            checkArgument(isValidCoordinatorSpec(h), "%s is not a valid host spec", h);
            sbld.add(HostAndPort.fromString(h).withDefaultPort(Constants.DEFAULT_INTERNAL_PORT).toString());
        }
        return sbld.build();
    }

    /**
     * Convenience method mainly used in local cluster testing
     *
     * @param ports a list of ports
     * @return a set of coordinator specs
     */
    public static ImmutableSortedSet<String> hosts(int...ports) {
        if (ports.length == 0) {
            return ImmutableSortedSet.of(
                    HostAndPort.fromParts("", Constants.DEFAULT_INTERNAL_PORT).toString());
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
                .withDefaultPort(Constants.DEFAULT_INTERNAL_PORT);
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

    public static final Predicate<Integer> in(final Set<Integer> set) {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return set.contains(input);
            }
        };
    }

    public static MeshProber prober(HostMessenger hm) {
        return MeshProber.class.cast(hm.getAcceptor());
    }

    protected final ImmutableSortedSet<String> m_coordinators;
    protected final VersionChecker m_versionChecker;
    protected final boolean m_enterprise;
    protected final StartAction m_startAction;
    /** {@code true} if there are no recoverable artifacts (Command Logs, Snapshots) */
    protected final boolean m_bare;
    protected final UUID m_configHash;
    protected final UUID m_meshHash;
    protected final Supplier<Integer> m_hostCountSupplier;
    protected final int m_kFactor;
    protected final boolean m_paused;
    protected final Supplier<NodeState> m_nodeStateSupplier;
    protected final boolean m_addAllowed;
    protected final boolean m_safeMode;
    protected final String m_terminusNonce;
    protected final HostCriteriaRef m_hostCriteria = new HostCriteriaRef();
    /*
     * on probe startup mode this future is set when there are enough
     * hosts to matched the configured cluster size
     */
    private final SettableFuture<Determination> m_probedDetermination =
            SettableFuture.create();

    protected MeshProber(NavigableSet<String> coordinators,
            VersionChecker versionChecker, boolean enterprise, StartAction startAction,
            boolean bare, UUID configHash, Supplier<Integer> hostCountSupplier,
            int kFactor, boolean paused, Supplier<NodeState> nodeStateSupplier,
            boolean addAllowed, boolean safeMode, String terminusNonce) {

        checkArgument(versionChecker != null, "version checker is null");
        checkArgument(configHash != null, "config hash is null");
        checkArgument(startAction != null, "start action is null");
        checkArgument(nodeStateSupplier != null, "nodeStateSupplier is null");
        checkArgument(hostCountSupplier != null, "hostCountSupplier is null");
        checkArgument(kFactor >= 0, "invalid kFactor value: %s", kFactor);
        checkArgument(coordinators != null &&
                coordinators.stream().allMatch(h->isValidCoordinatorSpec(h)),
                "coordinators is null or contains invalid host/interface specs %s", coordinators);
        checkArgument(coordinators.size() <= hostCountSupplier.get(),
                "host count %s is less then the number of coordinators %s",
                hostCountSupplier.get(), coordinators.size());
        checkArgument(terminusNonce == null || !terminusNonce.trim().isEmpty(),
                "terminus should not be blank");

        this.m_coordinators = ImmutableSortedSet.copyOf(coordinators);
        this.m_versionChecker = versionChecker;
        this.m_enterprise = enterprise;
        this.m_startAction = startAction;
        this.m_bare = bare;
        this.m_configHash = configHash;
        this.m_hostCountSupplier = hostCountSupplier;
        this.m_kFactor = kFactor;
        this.m_paused = paused;
        this.m_nodeStateSupplier = nodeStateSupplier;
        this.m_addAllowed = addAllowed;
        this.m_safeMode = safeMode;
        this.m_terminusNonce = terminusNonce;

        this.m_meshHash = Digester.md5AsUUID("hostCount="+ hostCountSupplier.get() + '|' + this.m_coordinators.toString());
    }

    public UUID getMeshHash() {
        return m_meshHash;
    }

    @Override
    public NavigableSet<String> getCoordinators() {
        return m_coordinators;
    }

    public NodeState getNodeState() {
        return m_nodeStateSupplier.get();
    }

    @Override
    public HostAndPort getLeader() {
        return HostAndPort.fromString(m_coordinators.first()).withDefaultPort(Constants.DEFAULT_INTERNAL_PORT);
    }

    @Override
    public VersionChecker getVersionChecker() {
        return m_versionChecker;
    }

    public boolean isEnterprise() {
        return m_enterprise;
    }

    public StartAction getStartAction() {
        return m_startAction;
    }

    /**
     * @return {@code true} if there are no recoverable artifacts (Command Logs, Snapshots)
     */
    public boolean isBare() {
        return m_bare;
    }

    public UUID getConfigHash() {
        return m_configHash;
    }

    public int getHostCount() {
        return m_hostCountSupplier.get();
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

    public boolean isSafeMode() {
        return m_safeMode;
    }

    public String getTerminusNonce() {
        return m_terminusNonce;
    }

    public HostCriteria asHostCriteria() {
        return new HostCriteria(
                m_paused,
                m_configHash,
                m_meshHash,
                m_enterprise,
                m_startAction,
                m_bare,
                m_hostCountSupplier.get(),
                m_nodeStateSupplier.get(),
                m_addAllowed,
                m_safeMode,
                m_terminusNonce
                );
    }

    public HostCriteria asHostCriteria(boolean paused) {
        return new HostCriteria(
                paused,
                m_configHash,
                m_meshHash,
                m_enterprise,
                m_startAction,
                m_bare,
                m_hostCountSupplier.get(),
                m_nodeStateSupplier.get(),
                m_addAllowed,
                m_safeMode,
                m_terminusNonce
                );
    }

    @Override
    public void detract(int hostId) {
        checkArgument(hostId >= 0, "host id %s is not greater or equal to 0", hostId);
        Map<Integer,HostCriteria> expect;
        Map<Integer,HostCriteria> update;

        do {
            expect = m_hostCriteria.get();
            update = ImmutableMap.<Integer, HostCriteria>builder()
                .putAll(Maps.filterKeys(expect, not(equalTo(hostId))))
                .build();
        } while (!m_hostCriteria.compareAndSet(expect, update));
    }

    @Override
    public void detract(Set<Integer> hostIds) {
        checkArgument(hostIds != null, "set of host ids is null");
        Map<Integer,HostCriteria> expect;
        Map<Integer,HostCriteria> update;

        do {
            expect = m_hostCriteria.get();
            update = ImmutableMap.<Integer, HostCriteria>builder()
                .putAll(Maps.filterKeys(expect, not(in(hostIds))))
                .build();
        } while (!m_hostCriteria.compareAndSet(expect, update));
    }

    @Override
    public JSONObject decorate(JSONObject jo, Optional<Boolean> paused) {
        return paused.map(p -> asHostCriteria(p).appendTo(jo)).orElse(asHostCriteria().appendTo(jo));
    }

    @Override
    public JoinAcceptor.PleaDecision considerMeshPlea(ZooKeeper zk, int hostId, JSONObject jo) {
        checkArgument(zk != null, "zookeeper is null");
        checkArgument(jo != null, "json object is null");

        if (!HostCriteria.hasCriteria(jo)) {
            return new JoinAcceptor.PleaDecision(
                    String.format("Joining node version %s is incompatible with this node verion %s",
                            jo.optString(SocketJoiner.VERSION_STRING,"(unknown)"),
                            m_versionChecker.getVersionString()),
                    false,
                    false);
        }

        HostCriteria hc = new HostCriteria(jo);
        Map<Integer,HostCriteria> hostCriteria = m_hostCriteria.get();

        // host criteria must be strictly compatible only if no node is operational (i.e.
        // when the cluster is forming anew)
        if (   !getNodeState().operational()
            && !hostCriteria.values().stream().anyMatch(c->c.getNodeState().operational())) {
            List<String> incompatibilities = asHostCriteria().listIncompatibilities(hc);
            if (!incompatibilities.isEmpty()) {
                Joiner joiner = Joiner.on("\n    ").skipNulls();
                String error = "Incompatible joining criteria:\n    "
                        + joiner.join(incompatibilities);
                return new JoinAcceptor.PleaDecision(error, false, false);
            }
            return new JoinAcceptor.PleaDecision(null, true, false);
        } else {
            StartAction operationalStartAction = hostCriteria.values().stream()
                    .filter(c->c.getNodeState().operational())
                    .map(c->c.getStartAction())
                    .findFirst().orElse(getStartAction());
            if (operationalStartAction == StartAction.PROBE && hc.getStartAction() != StartAction.PROBE) {
                String msg = "Invalid VoltDB command. Please use init and start to join this cluster";
                return new JoinAcceptor.PleaDecision(msg, false, false);
            }
        }
        // how many hosts are already in the mesh?
        Stat stat = new Stat();
        try {
            zk.getChildren(CoreZK.hosts, false, stat);
        } catch (InterruptedException e) {
            String msg = "Interrupted while considering mesh plea";
            m_networkLog.error(msg, e);
            return new JoinAcceptor.PleaDecision(msg, false, false);
        } catch (KeeperException e) {
            EnumSet<KeeperException.Code> closing = EnumSet.of(
                    KeeperException.Code.SESSIONEXPIRED,
                    KeeperException.Code.CONNECTIONLOSS);
            if (closing.contains(e.code())) {
                return new JoinAcceptor.PleaDecision("Shutting down", false, false);
            } else {
                String msg = "Failed to list hosts while considering a mesh plea";
                m_networkLog.error(msg, e);
                return new JoinAcceptor.PleaDecision(msg, false, false);
            }
        }
        // connecting to already wholly formed cluster
        if (stat.getNumChildren() >= getHostCount()) {
            return new JoinAcceptor.PleaDecision(
                    hc.isAddAllowed()? null : "Cluster is already complete",
                    hc.isAddAllowed(), false);
        } else if (stat.getNumChildren() < getHostCount()) {
            // check for concurrent rejoins
            final int rejoiningHost = CoreZK.createRejoinNodeIndicator(zk, hostId);

            if (rejoiningHost == -1) {
                return new JoinAcceptor.PleaDecision(null, true, false);
            } else {
                String msg = "Only one host can rejoin at a time. Host "
                      + rejoiningHost + " is still rejoining.";
                return new JoinAcceptor.PleaDecision(msg, false, true);
            }
        }
        return new JoinAcceptor.PleaDecision(null, true, false);
    }

    @Override
    public void accrue(int hostId, JSONObject jo) {
        checkArgument(hostId >= 0, "host id %s is not greater or equal to 0", hostId);
        checkArgument(jo != null, "json object is null");

        HostCriteria hc = new HostCriteria(jo);
        checkArgument(!hc.isUndefined(), "json object does not contain host prober fields");

        Map<Integer,HostCriteria> expect;
        Map<Integer,HostCriteria> update;

        do {
            expect = m_hostCriteria.get();
            update = ImmutableMap.<Integer, HostCriteria>builder()
                .putAll(Maps.filterKeys(expect, not(equalTo(hostId))))
                .put(hostId, hc)
                .build();

        } while (!m_hostCriteria.compareAndSet(expect, update));

        determineStartActionIfNecessary(update);
    }

    @Override
    public void accrue(Map<Integer, JSONObject> jos) {
        checkArgument(jos != null, "map of host ids and json object is null");

        ImmutableMap.Builder<Integer, HostCriteria> hcb = ImmutableMap.builder();
        for (Map.Entry<Integer, JSONObject> e: jos.entrySet()) {
            HostCriteria hc = new HostCriteria(e.getValue());
            checkArgument(!hc.isUndefined(), "json boject for host id %s does not contain prober fields", e.getKey());
            hcb.put(e.getKey(), hc);
        }
        Map<Integer, HostCriteria> additions = hcb.build();

        Map<Integer,HostCriteria> expect;
        Map<Integer,HostCriteria> update;

        do {
            expect = m_hostCriteria.get();
            update = ImmutableMap.<Integer, HostCriteria>builder()
                .putAll(Maps.filterKeys(expect, not(in(additions.keySet()))))
                .putAll(additions)
                .build();

        } while (!m_hostCriteria.compareAndSet(expect, update));

        determineStartActionIfNecessary(update);
    }

    /**
     * Check to see if we have enough {@link HostCriteria} gathered to make a
     * start action {@link Determination}
     */
    private void determineStartActionIfNecessary(Map<Integer, HostCriteria> hostCriteria) {
        // already made a determination
        if (m_probedDetermination.isDone()) {
            return;
        }
        final int ksafety = getkFactor() + 1;

        int bare = 0; // node has no recoverable artifacts (Command Logs, Snapshots)
        int unmeshed = 0;
        int operational = 0;
        int haveTerminus = 0;
        int hostCount = getHostCount();

        // both paused and safemode need to be specified on only one node to
        // make them a cluster attribute. These are overridden if there are
        // any nodes in operational state
        boolean paused = isPaused();
        boolean safemode = isSafeMode();

        final NavigableSet<String> terminusNonces = new TreeSet<>();

        for (HostCriteria c: hostCriteria.values()) {
            if (c.getNodeState().operational()) {
                operational += 1;
                // pause state from operational nodes overrides yours
                // prefer host count from operational nodes
                if (operational == 1) {
                    paused = c.isPaused();
                    hostCount = c.getHostCount();
                }
            }
            unmeshed += c.getNodeState().unmeshed() ? 1 : 0;
            bare += c.isBare() ? 1 : 0;
            if (c.isPaused() && operational == 0) {
                paused = c.isPaused();
            }
            safemode = safemode || c.isSafeMode();
            if (c.getTerminusNonce() != null) {
                terminusNonces.add(c.getTerminusNonce());
                ++haveTerminus;
            }
        }
        // not enough host criteria to make a determination
        if (hostCriteria.size() < hostCount && operational == 0) {
            m_networkLog.debug("have yet to receive all the required host criteria");
            return;
        }
        // handle add (i.e. join) cases too
        if (hostCount < getHostCount() && hostCriteria.size() <= hostCount) {
            m_networkLog.debug("have yet to receive all the required host criteria");
            return;
        }

        m_networkLog.debug("Received all the required host criteria");

        safemode = safemode && operational == 0 && bare < ksafety; // kfactor + 1

        if (m_networkLog.isDebugEnabled()) {
            m_networkLog.debug("We have "
                    + operational + " operational nodes, "
                    + bare + " bare nodes, and "
                    + unmeshed + " unmeshed nodes");
            m_networkLog.debug("Propagated cluster attribute are paused: "
                    + paused + ", and safemode: " + safemode);
        }

        if (terminusNonces.size() > 1) {
            org.voltdb.VoltDB.crashLocalVoltDB("Detected multiple startup snapshots, cannot "
                    + "proceed with cluster startup. Snapshot IDs " + terminusNonces);
        }

        String terminusNonce = terminusNonces.pollFirst();
        if (operational == 0 && haveTerminus <= (hostCount - ksafety)) {
            terminusNonce = null;
        }

        if (getStartAction() != StartAction.PROBE) {
            m_probedDetermination.set(new Determination(
                    getStartAction(), getHostCount(), paused, terminusNonce));
            return;
        }

        StartAction determination = isBare() ?
                  StartAction.CREATE
                : StartAction.RECOVER;

        if (operational > 0 && operational < hostCount) { // rejoin
            determination = StartAction.LIVE_REJOIN;
        } else if (operational > 0 && operational == hostCount) { // join
            if (isAddAllowed()) {
                hostCount = hostCount + ksafety; // kfactor + 1
                determination = StartAction.JOIN;
            } else {
                org.voltdb.VoltDB.crashLocalVoltDB("Node is not allowed to rejoin an already complete cluster");
                return;
            }
        } else if (operational == 0 && bare == unmeshed) {
            determination = StartAction.CREATE;
        } else if (operational == 0 && bare < ksafety /* kfactor + 1 */) {
            determination = safemode ? StartAction.SAFE_RECOVER : StartAction.RECOVER;
        } else if (operational == 0 && bare >= ksafety  /* kfactor + 1 */) {
            org.voltdb.VoltDB.crashLocalVoltDB("Cluster has incomplete command logs: "
                    + bare + " nodes have no command logs, while "
                    + (unmeshed - bare) + " nodes have them");
            return;
        }

        final Determination dtrm = new Determination(determination, hostCount, paused, terminusNonce);
        if (m_networkLog.isDebugEnabled()) {
            m_networkLog.debug("made the following " + dtrm);
        }
        m_probedDetermination.set(dtrm);
    }

    public Determination waitForDetermination() {
        try {
            return m_probedDetermination.get();
        } catch (ExecutionException notThrownBecauseItIsASettableFuture) {
        } catch (InterruptedException e) {
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "interrupted while waiting to determine the cluster start action",
                    false, e);
        }
        return new Determination(null,-1, false, null);
    }

    @Generated("by eclipse's equals and hashCode source generators")
    @Override
    public String toString() {
        return "MeshProber [coordinators=" + m_coordinators
                + ", enterprise=" + m_enterprise + ", startAction=" + m_startAction
                + ", bare=" + m_bare + ", configHash=" + m_configHash
                + ", meshHash=" + m_meshHash + ", hostCount=" + m_hostCountSupplier.get()
                + ", kFactor=" + m_kFactor + ", paused=" + m_paused
                + ", addAllowed=" + m_addAllowed + ", safeMode=" + m_safeMode + "]";
    }

    public void appendTo(JSONWriter jw) throws JSONException {
        jw.object();
        jw.key(COORDINATORS).array();
        for (String coordinator: m_coordinators) {
            jw.value(coordinator);
        }
        jw.endArray();
        jw.keySymbolValuePair(ENTERPRISE, m_enterprise);
        jw.keySymbolValuePair(START_ACTION, m_startAction.name());
        jw.keySymbolValuePair(BARE, m_bare);
        jw.keySymbolValuePair(CONFIG_HASH, m_configHash.toString());
        jw.keySymbolValuePair(MESH_HASH, m_meshHash.toString());
        jw.keySymbolValuePair(HOST_COUNT, m_hostCountSupplier.get());
        jw.keySymbolValuePair(K_FACTOR, m_kFactor);
        jw.keySymbolValuePair(PAUSED, m_paused);
        jw.keySymbolValuePair(ADD_ALLOWED, m_addAllowed);
        jw.keySymbolValuePair(SAFE_MODE, m_safeMode);
        jw.keySymbolValuePair(TERMINUS_NONCE, m_terminusNonce);
        jw.endObject();
    }

    @Override
    public String toJSONString() {
        JSONStringer js = new JSONStringer();
        try {
            appendTo(js);
        } catch (JSONException e) {
            Throwables.propagate(e);
        }
        return js.toString();
    }

    public static class Determination {
        public final StartAction startAction;
        public final int hostCount;
        public final boolean paused;
        public final String terminusNonce;

        private Determination(StartAction startAction, int hostCount,
                boolean paused, String terminusNonce) {
            this.startAction = startAction;
            this.hostCount = hostCount;
            this.paused = paused;
            this.terminusNonce = terminusNonce;
        }

        @Override @Generated("by eclipse's equals and hashCode source generators")
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + hostCount;
            result = prime * result + (paused ? 1231 : 1237);
            result = prime * result
                    + ((startAction == null) ? 0 : startAction.hashCode());
            result = prime * result
                    + ((terminusNonce == null) ? 0 : terminusNonce.hashCode());
            return result;
        }

        @Override @Generated("by eclipse's equals and hashCode source generators")
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Determination other = (Determination) obj;
            if (hostCount != other.hostCount)
                return false;
            if (paused != other.paused)
                return false;
            if (startAction != other.startAction)
                return false;
            if (terminusNonce == null) {
                if (other.terminusNonce != null)
                    return false;
            } else if (!terminusNonce.equals(other.terminusNonce))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "Determination [startAction=" + startAction + ", hostCount="
                    + hostCount + ", paused=" + paused + ", terminusNonce=" + terminusNonce + "]";
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected final VersionChecker m_defaultVersionChecker = JoinAcceptor.DEFAULT_VERSION_CHECKER;

        protected NavigableSet<String> m_coordinators = ImmutableSortedSet.of("localhost");
        protected VersionChecker m_versionChecker = m_defaultVersionChecker;
        protected boolean m_enterprise = MiscUtils.isPro();
        protected StartAction m_startAction = StartAction.PROBE;
        protected boolean m_bare = true;
        protected UUID m_configHash = new UUID(0L, 0L);
        protected Supplier<Integer> m_hostCountSupplier;
        protected int m_kFactor = 0;
        protected boolean m_paused = false;
        protected Supplier<NodeState> m_nodeStateSupplier =
                Suppliers.ofInstance(NodeState.INITIALIZING);
        protected boolean m_addAllowed = false;
        protected boolean m_safeMode = false;
        protected String m_terminusNonce = null;

        protected Builder() {
        }

        public Builder prober(MeshProber o) {
            m_coordinators = ImmutableSortedSet.copyOf(checkNotNull(o).m_coordinators);
            m_versionChecker = o.m_versionChecker;
            m_enterprise = o.m_enterprise;
            m_startAction = o.m_startAction;
            m_bare = o.m_bare;
            m_configHash = o.m_configHash;
            m_hostCountSupplier = o.m_hostCountSupplier;
            m_kFactor = o.m_kFactor;
            m_paused = o.m_paused;
            m_nodeStateSupplier = o.m_nodeStateSupplier;
            m_addAllowed = o.m_addAllowed;
            m_safeMode = o.m_safeMode;
            m_terminusNonce = o.m_terminusNonce;
            return this;
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
            m_hostCountSupplier = Suppliers.ofInstance(hostCount);
            return this;
        }

        public Builder hostCountSupplier(Supplier<Integer> supplier) {
            m_hostCountSupplier = supplier;
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

        public Builder safeMode(boolean safeMode) {
            m_safeMode = safeMode;
            return this;
        }

        public Builder terminusNonce(String terminusNonce) {
            m_terminusNonce = terminusNonce;
            return this;
        }

        public MeshProber build() {
            if (m_hostCountSupplier == null && m_coordinators != null) {
                m_hostCountSupplier = Suppliers.ofInstance(m_coordinators.size());
            }
            return new MeshProber(
                    m_coordinators,
                    m_versionChecker,
                    m_enterprise,
                    m_startAction,
                    m_bare,
                    m_configHash,
                    m_hostCountSupplier,
                    m_kFactor,
                    m_paused,
                    m_nodeStateSupplier,
                    m_addAllowed,
                    m_safeMode,
                    m_terminusNonce
                    );
        }
    }

    final static class HostCriteriaRef extends AtomicReference<Map<Integer, HostCriteria>> {
        private static final long serialVersionUID = -7947013480687680553L;

        final static Map<Integer,HostCriteria> EMPTY_MAP = ImmutableMap.of();

        public HostCriteriaRef(Map<Integer, HostCriteria> map) {
            super(map);
        }

        public HostCriteriaRef() {
            this(EMPTY_MAP);
        }
    }
 }
