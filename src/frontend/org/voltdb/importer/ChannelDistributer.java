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

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Preconditions.checkState;
import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.isNull;
import static com.google_voltpatches.common.base.Predicates.not;
import static com.google_voltpatches.common.base.Predicates.or;
import static org.voltcore.zk.ZKUtil.joinZKPath;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.AsyncCallback.Children2Callback;
import org.apache.zookeeper_voltpatches.AsyncCallback.DataCallback;
import org.apache.zookeeper_voltpatches.AsyncCallback.StatCallback;
import org.apache.zookeeper_voltpatches.AsyncCallback.StringCallback;
import org.apache.zookeeper_voltpatches.AsyncCallback.VoidCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.Code;
import org.apache.zookeeper_voltpatches.KeeperException.NodeExistsException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.EventType;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooDefs;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.collect.TreeMultimap;
import com.google_voltpatches.common.eventbus.AsyncEventBus;
import com.google_voltpatches.common.eventbus.DeadEvent;
import com.google_voltpatches.common.eventbus.EventBus;
import com.google_voltpatches.common.eventbus.Subscribe;
import com.google_voltpatches.common.eventbus.SubscriberExceptionContext;
import com.google_voltpatches.common.eventbus.SubscriberExceptionHandler;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 *  An importer channel distributer that uses zookeeper to coordinate how importer channels are
 *  distributed among VoltDB cluster nodes. A proposal is merged against a master channel lists,
 *  stored in the /import/master zookeeper node. The elected distributer leader distributes the
 *  merge differences among all available nodes, by writing each node's assigned list of
 *  channels in their respective /import/host/[host-name] nodes. When a node leaves the mesh
 *  its assigned channels are redistributed among the surviving nodes
 */
public class ChannelDistributer implements ChannelChangeCallback {

    private final static VoltLogger LOG = new VoltLogger("IMPORT");

    /** root for all importer nodes */
    static final String IMPORT_DN = "/import";
    /** parent node for all {@link CreateMode#EPHEMERAL ephemeral} host nodes */
    static final String HOST_DN = joinZKPath(IMPORT_DN, "host");
    /** parent directory for leader candidates and holder of the channels master list */
    static final String MASTER_DN = joinZKPath(IMPORT_DN, "master");
    /** leader candidate node prefix for {@link CreateMode#EPHEMERAL_SEQUENTIAL} nodes */
    static final String CANDIDATE_PN = joinZKPath(MASTER_DN, "candidate_");

    static final byte[] EMPTY_ARRAY = "[]".getBytes(StandardCharsets.UTF_8);

    static void mkdirs(ZooKeeper zk, String zkNode, byte[] content) {
        try {
            ZKUtil.asyncMkdirs(zk, zkNode, content).get();
        } catch (NodeExistsException itIsOk) {
        } catch (InterruptedException | KeeperException e) {
            String msg = "Unable to create zk directory: " + zkNode;
            LOG.error(msg, e);
            throw new DistributerException(msg, e);
        }
    }

    /**
     * Boiler plate method to log an error message and wrap, and return a {@link DistributerException}
     * around the message and cause
     *
     * @param cause fault origin {@link Throwable}
     * @param format a {@link String#format(String, Object...) compliant format string
     * @param args formatter arguments
     * @return a {@link DistributerException}
     */
    static DistributerException loggedDistributerException(Throwable cause, String format, Object...args) {
        Optional<DistributerException> causeFor = DistributerException.isCauseFor(cause);
        if (causeFor.isPresent()) {
            return causeFor.get();
        }
        String msg = String.format(format, args);
        if (cause != null) {
            LOG.error(msg, cause);
            return new DistributerException(msg, cause);
        } else {
            LOG.error(msg);
            return new DistributerException(msg);
        }
    }

    /**
     * Boiler plate method that checks a zookeeper callback @{link {@link KeeperException.Code},
     * converts it to a {@link DistributerException} and if it does not indicate success,
     *
     * @param code a {@link KeeperException.Code callback code}
     * @param format {@link String#format(String, Object...) compliant format string
     * @param args formatter arguments
     * @return an {@link Optional} that may contain a {@link DistributerException}
     */
    static Optional<DistributerException> checkCode(Code code, String format, Object...args) {
        if (code != Code.OK) {
            KeeperException kex = KeeperException.create(code);
            return Optional.of(loggedDistributerException(kex, format, args));
        } else {
            return Optional.absent();
        }
    }

    /**
     * Boiler plate method that acquires, and releases a {@link Semaphore}
     * @param lock a {@link Semaphore}
     */
    static void acquireAndRelease(Semaphore lock) {
        try {
            lock.acquire();
            lock.release();
        } catch (InterruptedException ex) {
            throw loggedDistributerException(ex, "interruped while waiting for a semaphare");
        }
    }

    /**
     * Reads the JSON document contained in the byte array data, and
     * converts it to a {@link NavigableSet<ChannelSpec> set of channel specs}
     *
     * @param data zookeeper node data content
     * @return a {@link NavigableSet<ChannelSpec> set of channel specs}
     * @throws JSONException on JSON parse failures
     * @throws IllegalArgumentException on encoded channel spec parse failures
     */
    static NavigableSet<ChannelSpec> asChannelSet(byte[] data)
            throws JSONException, IllegalArgumentException {
        ImmutableSortedSet.Builder<ChannelSpec> sbld = ImmutableSortedSet.naturalOrder();
        JSONArray ja = new JSONArray(new String(data, StandardCharsets.UTF_8));
        for (int i=0; i< ja.length(); ++i) {
            sbld.add(new ChannelSpec(ja.getString(i)));
        }
        return  sbld.build();
    }

    /**
     * Converts the given a {@link NavigableSet<ChannelSpec> set of channel specs}
     * into a byte array with the content of a JSON document
     *
     * @param specs a a {@link NavigableSet<ChannelSpec> set of channel specs}
     * @return a byte array
     * @throws JSONException on JSON building failures
     * @throws IllegalArgumentException on channel spec encoding failures
     */
    static byte [] asHostData(NavigableSet<ChannelSpec> specs)
            throws JSONException, IllegalArgumentException {
        JSONStringer js = new JSONStringer();
        js.array();
        for (ChannelSpec spec: specs) {
            js.value(spec.asJSONValue());
        }
        js.endArray();
        return js.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Tracing utility method useful for debugging
     *
     * @param o and object
     * @return string with information on the given object
     */
    static String id(Object o) {
        if (o == null) return "(null)";
        Thread t = Thread.currentThread();
        StringBuilder sb = new StringBuilder(128);
        sb.append("(T[").append(t.getName()).append("]@");
        sb.append(Long.toString(t.getId(), Character.MAX_RADIX));
        sb.append(":O[").append(o.getClass().getSimpleName());
        sb.append("]@");
        sb.append(Long.toString(System.identityHashCode(o),Character.MAX_RADIX));
        sb.append(")");
        return sb.toString();
    }

    final static SubscriberExceptionHandler eventBusFaultHandler = new SubscriberExceptionHandler() {
        @Override
        public void handleException(Throwable exception, SubscriberExceptionContext context) {
            loggedDistributerException(
                    exception,
                    "fault during callback dispatch for event %s",
                    context.getEvent()
                    );
        }
    };

    private final ExecutorService m_es;
    private final AtomicBoolean m_done = new AtomicBoolean(false);
    private final ZooKeeper m_zk;
    private final String m_hostId;
    private final String m_candidate;
    private final Deque<ImporterChannelAssignment> m_undispatched;
    private final EventBus m_eb;
    private final ExecutorService m_buses;

    volatile boolean m_isLeader = false;
    final SpecsRef m_specs = new SpecsRef();
    final HostsRef m_hosts = new HostsRef();
    final ChannelsRef m_channels = new ChannelsRef();
    final CallbacksRef m_callbacks = new CallbacksRef();
    final UnregisteredRef m_unregistered = new UnregisteredRef();
    final AtomicStampedReference<OperationMode> m_mode;

    /**
     * Initialize a distributer within importer channel distribution mesh by performing the
     * following actions:
     * <ul>
     * <li>registers a leader candidate</li>
     * <li>starts watchers on the channel master list, on the directory holding host nodes,
     * and the directory used for leader elections</li>
     * <li>create election candidate node</li>
     * <li>create its own host node</li>
     * </ul>
     * @param zk
     * @param hostId
     * @param queue
     */
    public ChannelDistributer(ZooKeeper zk, String hostId) {
        Preconditions.checkArgument(
                hostId != null && !hostId.trim().isEmpty(),
                "hostId is null or empty"
                );
        m_hostId = hostId;
        m_zk = Preconditions.checkNotNull(zk, "zookeeper is null");
        m_es = CoreUtils.getCachedSingleThreadExecutor("Import Channel Distributer for Host " + hostId, 15000);
        m_buses = CoreUtils.getCachedSingleThreadExecutor(
                "Import Channel Distributer Event Bus Dispatcher for Host " + hostId, 15000
                );
        m_eb = new AsyncEventBus(m_buses, eventBusFaultHandler);
        m_eb.register(this);
        m_mode = new AtomicStampedReference<>(OperationMode.RUNNING, 0);
        m_undispatched = new LinkedList<>();

        // Prime directory structure if needed
        OperationMode startMode = VoltDB.instance().getStartMode();
        assert startMode == OperationMode.RUNNING || startMode == OperationMode.PAUSED;
        mkdirs(zk, VoltZK.operationMode, startMode.getBytes());
        mkdirs(zk, HOST_DN, EMPTY_ARRAY);
        mkdirs(zk, MASTER_DN, EMPTY_ARRAY);

        GetOperationMode opMode = new GetOperationMode(VoltZK.operationMode);
        CreateNode createHostNode = new CreateNode(
                joinZKPath(HOST_DN, hostId),
                EMPTY_ARRAY, CreateMode.EPHEMERAL
                );
        MonitorHostNodes monitor = new MonitorHostNodes(HOST_DN);
        CreateNode electionCandidate = new CreateNode(
                CANDIDATE_PN,
                EMPTY_ARRAY, CreateMode.EPHEMERAL_SEQUENTIAL
                );
        ElectLeader elector = new ElectLeader(MASTER_DN, electionCandidate);

        createHostNode.getNode();
        elector.elect();

        m_candidate = electionCandidate.getNode();
        opMode.getMode();
        monitor.getChildren();
        // monitor the master list
        new GetChannels(MASTER_DN).getChannels();
    }

    public String getHostId() {
        return m_hostId;
    }

    public VersionedOperationMode getOperationMode() {
        return new VersionedOperationMode(m_mode);
    }

    /**
     * Register channels for the given importer. If they match to what is already registered
     * then nothing is done. Before registering channels, you need to register a callback
     * handler for channel assignments {@link #registerCallback(String, ChannelChangeCallback)}
     *
     * @param importer importer designation
     * @param uris list of channel URIs
     */
    public void registerChannels(String importer, Set<URI> uris) {
        NavigableSet<String> registered = m_callbacks.getReference().navigableKeySet();
        Preconditions.checkArgument(
                importer != null && !importer.trim().isEmpty(),
                "importer is null or empty"
                );
        Preconditions.checkArgument(uris != null, "uris set is null");
        Preconditions.checkArgument(
                !FluentIterable.from(uris).anyMatch(isNull()),
                "uris set %s contains null elements", uris
                );
        if (!registered.contains(importer)) {
            if (uris.isEmpty()) {
                // ImporterLifeCycleManager.stop() calls registerChannels() is called with an empty set of URIs.
                // If the importer never finished starting, we hit this condition.
                // This log message is used by the TestImporterStopAfterIncompleteStart JUnit.
                LOG.info("Skipping channel un-registration for " + importer + " since it did not finish initialization");
                return;
            } else {
                throw new IllegalStateException("no callbacks registered for " + importer
                        + " - unable to register channels " + Arrays.toString(uris.toArray()));
            }
        }

        Predicate<ChannelSpec> forImporter = ChannelSpec.importerIs(importer);
        Function<URI,ChannelSpec> asSpec = ChannelSpec.fromUri(importer);

        // convert method parameters to a set of ChannelSpecs
        NavigableSet<ChannelSpec> proposed = ImmutableSortedSet.copyOf(
                FluentIterable.from(uris).transform(asSpec)
                );

        LOG.info("(" + m_hostId + ") proposing channels " + proposed);

        int [] stamp = new int[]{0};

        ImmutableSortedSet.Builder<ChannelSpec> sbldr = null;
        NavigableSet<ChannelSpec> prev = null;
        SetData setter = null;
        NavigableSet<ChannelSpec> masterList = null;
        // retry writes when merging with stale data
        do {
            prev = m_channels.get(stamp);

            NavigableSet<ChannelSpec> current  = Sets.filter(prev, forImporter);
            if (current.equals(proposed)) {
                return;
            }
            sbldr = ImmutableSortedSet.naturalOrder();
            sbldr.addAll(Sets.filter(prev, not(forImporter)));
            sbldr.addAll(proposed);

            byte [] data = null;
            try {
                masterList = sbldr.build();
                data = asHostData(masterList);
            } catch (JSONException|IllegalArgumentException e) {
                throw loggedDistributerException(e, "failed to serialize the registration as json");
            }

            setter = new SetData(MASTER_DN, stamp[0], data);
        } while (setter.getCallbackCode() == Code.BADVERSION);

        //synch master channel list after channel registrations.
        int [] sstamp = new int[]{0};
        prev = m_channels.get(sstamp);
        m_channels.compareAndSet(prev, masterList, sstamp[0], stamp[0]);

        setter.getStat();
    }

    /**
     * Registers a (@link ChannelChangeCallback} for the given importer.
     * @param importer
     * @param callback a (@link ChannelChangeCallback}
     */
    public void registerCallback(String importer, ChannelChangeCallback callback) {
        Preconditions.checkArgument(
                importer != null && !importer.trim().isEmpty(),
                "importer is null or empty"
                );
        callback = checkNotNull(callback, "callback is null");

        if (m_done.get()) return;

        int [] stamp = new int[]{0};
        NavigableMap<String,ChannelChangeCallback> prev = null;
        NavigableMap<String,ChannelChangeCallback> next = null;
        ImmutableSortedMap.Builder<String,ChannelChangeCallback> mbldr = null;

        synchronized (m_undispatched) {
            do {
                prev = m_callbacks.get(stamp);
                mbldr = ImmutableSortedMap.naturalOrder();
                mbldr.putAll(Maps.filterKeys(prev, not(equalTo(importer))));
                mbldr.put(importer, callback);
                next = mbldr.build();
            } while (!m_callbacks.compareAndSet(prev, next, stamp[0], stamp[0]+1));

            NavigableSet<String> registered = next.navigableKeySet();
            NavigableSet<String> unregistered = m_unregistered.getReference();

            Iterator<ImporterChannelAssignment> itr = m_undispatched.iterator();
            while (itr.hasNext()) {
                final ImporterChannelAssignment assignment = itr.next();
                if (registered.contains(assignment.getImporter())) {
                    final ChannelChangeCallback dispatch = next.get(assignment.getImporter());
                    m_buses.submit(new DistributerRunnable() {
                        @Override
                        public void susceptibleRun() throws Exception {
                            dispatch.onChange(assignment);
                        }
                    });
                    itr.remove();
                } else if (unregistered.contains(assignment.getImporter())) {
                    itr.remove();
                    if (!assignment.getAdded().isEmpty()) {
                        LOG.warn("(" + m_hostId
                                + ") discarding assignment to unregistered importer "
                                + assignment);
                    }
                }
            }
        }
    }

    /**
     * Unregisters the callback assigned to given importer. Once it is
     * unregistered it can no longer be re-registered
     *
     * @param importer
     */
    public void unregisterCallback(String importer) {
        if (   importer == null
            || !m_callbacks.getReference().containsKey(importer)
            || m_unregistered.getReference().contains(importer))
        {
            return;
        }
        if (m_done.get()) return;

        int [] rstamp = new int[]{0};
        NavigableMap<String,ChannelChangeCallback> rprev = null;
        NavigableMap<String,ChannelChangeCallback> rnext = null;

        int [] ustamp = new int[]{0};
        NavigableSet<String> uprev = null;
        NavigableSet<String> unext = null;

        synchronized(m_undispatched) {
            do {
                rprev = m_callbacks.get(rstamp);
                rnext = ImmutableSortedMap.<String,ChannelChangeCallback>naturalOrder()
                        .putAll(Maps.filterKeys(rprev, not(equalTo(importer))))
                        .build();
            } while (rprev.containsKey(importer) && !m_callbacks.compareAndSet(rprev, rnext, rstamp[0], rstamp[0]+1));

            do {
                uprev = m_unregistered.get(ustamp);
                unext = ImmutableSortedSet.<String>naturalOrder()
                        .addAll(Sets.filter(uprev, not(equalTo(importer))))
                        .add(importer)
                        .build();
            } while (!uprev.contains(importer) && m_unregistered.compareAndSet(uprev, unext, ustamp[0], ustamp[0]+1));

            Iterator<ImporterChannelAssignment> itr = m_undispatched.iterator();
            while (itr.hasNext()) {
                final ImporterChannelAssignment assignment = itr.next();
                if (unext.contains(assignment.getImporter())) {
                    itr.remove();
                }
            }
        }
    }

    /**
     * Sets the done flag, shuts down its executor thread, and deletes its own host
     * and candidate nodes
     */
    public void shutdown() {
        if (m_done.compareAndSet(false, true)) {
            m_es.shutdown();
            m_buses.shutdown();
            DeleteNode deleteHost = new DeleteNode(joinZKPath(HOST_DN, m_hostId));
            DeleteNode deleteCandidate = new DeleteNode(m_candidate);
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw loggedDistributerException(e, "interrupted while waiting for executor termination");
            }
            try {
                m_buses.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw loggedDistributerException(e, "interrupted while waiting for executor termination");
            }
            deleteHost.onComplete();
            deleteCandidate.onComplete();
        }
    }

    /**
     * Keeps assignments for unregistered importers
     * @param e
     */
    @Subscribe
    public void undispatched(DeadEvent e) {
        if (!m_done.get() && e.getEvent() instanceof ImporterChannelAssignment) {
            ImporterChannelAssignment assignment = (ImporterChannelAssignment)e.getEvent();

            synchronized (m_undispatched) {
                NavigableSet<String> registered = m_callbacks.getReference().navigableKeySet();
                NavigableSet<String> unregistered = m_unregistered.getReference();
                if (registered.contains(assignment.getImporter())) {
                    m_eb.post(assignment);
                } else if (!assignment.getAdded().isEmpty()
                        && unregistered.contains(assignment.getImporter())) {
                    LOG.warn("(" + m_hostId
                            + ") disgarding assignment to unregistered importer "
                            + assignment);
                } else {
                    m_undispatched.add(assignment);
                }
            }
        }
    }

    @Override
    @Subscribe
    public void onChange(ImporterChannelAssignment assignment) {
        if (m_done.get()) {
            return;
        }
        ChannelChangeCallback cb = m_callbacks.getReference().get(assignment.getImporter());
        if (cb != null) {
            try {
                cb.onChange(assignment);
            } catch (Exception callbackException) {
                throw loggedDistributerException(
                        callbackException,
                        "failed to invoke the onChange() callback for importer %s",
                        assignment.getImporter()
                        );
            }
        } else if (   !assignment.getAdded().isEmpty()
                   && m_unregistered.getReference().contains(assignment.getImporter())) {
            LOG.warn("(" + m_hostId
                    + ") disgarding assignment to unregistered importer "
                    + assignment);
        } else {
            synchronized(m_undispatched) {
                m_undispatched.add(assignment);
            }
        }
    }

    @Override
    @Subscribe
    public void onClusterStateChange(VersionedOperationMode mode) {
        Optional<DistributerException> fault = Optional.absent();
        for (Map.Entry<String, ChannelChangeCallback> e: m_callbacks.getReference().entrySet()) try {
            if (!m_done.get()) e.getValue().onClusterStateChange(mode);
        } catch (Exception callbackException) {
            fault = Optional.of(loggedDistributerException(
                    callbackException,
                    "failed to invoke the onClusterStateChange() callback for importer %s",
                    e.getKey()
                    ));
        }
        if (fault.isPresent()) {
            throw fault.get();
        }
    }

    /**
     * Base class for all runnables submitted to the executor service
     */
    abstract class DistributerRunnable implements Runnable {
        @Override
        public void run() {
            try {
                if (!m_done.get()) {
                    susceptibleRun();
                }
            } catch (Exception ex) {
                throw loggedDistributerException(ex, "Fault occured while executing runnable");
            }
        }

        public abstract void susceptibleRun() throws Exception;
    }

    /**
     * A {@link DistributerRunnable} that compares the registered {@link ChannelSpec} list
     * against the already assigned list of {@link ChannelSpec}. Any additions are distributed
     * as evenly as possible to all the nodes participating in the distributer mesh, taking into
     * account the removals. Then it writes to each node their assigned list of importer
     * channels. This is run exclusively by the mesh leader.
     *
     */
    class AssignChannels extends DistributerRunnable {

        /** registered channels */
        final NavigableSet<ChannelSpec> channels = m_channels.getReference();
        /** assigned channels */
        final NavigableMap<ChannelSpec,String> specs = m_specs.getReference();
        /** mesh nodes */
        final NavigableMap<String,AtomicInteger> hosts = m_hosts.getReference();

        final int seed;

        AssignChannels() {
            seed = System.identityHashCode(this);
        }

        @Override
        public void susceptibleRun() throws Exception {
            if (m_mode.getReference() == OperationMode.INITIALIZING) {
                return;
            }

            NavigableSet<ChannelSpec> assigned = specs.navigableKeySet();
            Set<ChannelSpec> added   = Sets.difference(channels, assigned);
            Set<ChannelSpec> removed = Sets.difference(assigned, channels);

            if (added.isEmpty() && removed.isEmpty()) {
                return;
            }

            Predicate<Map.Entry<ChannelSpec,String>> withoutRemoved =
                    not(ChannelSpec.specKeyIn(removed, String.class));
            NavigableMap<ChannelSpec,String> pruned =
                    Maps.filterEntries(specs, withoutRemoved);

            if (!removed.isEmpty()) {
                LOG.info("LEADER (" + m_hostId + ") removing channels " + removed);
            }
            // makes it easy to group channels by host
            TreeMultimap<String, ChannelSpec> byhost = TreeMultimap.create();

            for (Map.Entry<ChannelSpec,String> e: pruned.entrySet()) {
                byhost.put(e.getValue(), e.getKey());
            }
            // approximation of how many channels should be assigned to each node
            int fair = new Double(Math.ceil(channels.size()/(double)hosts.size())).intValue();
            List<String> hostassoc = new ArrayList<>(added.size());
            for (String host: hosts.navigableKeySet()) {
                // negative means it is over allocated
                int room = fair - byhost.get(host).size();
                for (int i = 0; i < room; ++i) {
                    hostassoc.add(host);
                }
            }
            Collections.shuffle(hostassoc, new Random(seed));

            Iterator<String> hitr = hostassoc.iterator();
            Iterator<ChannelSpec> citr = added.iterator();
            while (citr.hasNext()) {
                String host = hitr.next();
                ChannelSpec spec = citr.next();
                byhost.put(host, spec);
                LOG.info("LEADER (" + m_hostId + ") assigning " + spec + " to host " + host);
            }

            try {
                // write to each node their assigned channel list
                NavigableSet<ChannelSpec> previous = null;
                NavigableSet<ChannelSpec> needed = null;
                List<SetNodeChannels> setters = new ArrayList<>();

                for (String host: hosts.navigableKeySet()) {
                    previous = Maps.filterValues(specs,equalTo(host)).navigableKeySet();
                    needed = byhost.get(host);
                    if (!needed.equals(previous)) {
                        int version = hosts.get(host).get();
                        byte [] nodedata = asHostData(needed);
                        setters.add(new SetNodeChannels(joinZKPath(HOST_DN, host), version, nodedata));
                    }
                }
                // wait for the last write to complete
                for (SetNodeChannels setter: setters) {
                    if (setter.getCallbackCode() != Code.OK && !m_done.get()) {
                        // NOTE: It's possible for AssignChannels to run twice in this scenario,
                        // once by MonitorHostNodes and once by GetChannels following a node loss event.
                        // This condition is rare, and better than having a scenario where AssignChannels is not run.
                        LOG.warn(
                                "LEADER (" + m_hostId
                                + ") Retrying channel assignment because write attempt to "
                                + setter.path + " failed with " + setter.getCallbackCode()
                               );
                        m_es.submit(new GetChannels(MASTER_DN));
                        return;
                    }
                }
            } catch (JSONException|IllegalArgumentException e) {
                LOG.fatal("unable to create json document to assign imported channels to nodes", e);
            }
        }
    }

    class ClusterTagCallback implements StatCallback {
        final SettableFuture<Stat> m_fut = SettableFuture.create();

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            Code code = Code.get(rc);
            if (code == Code.OK) {
                m_fut.set(stat);
            } else if (code != Code.NONODE) {
                KeeperException e = KeeperException.create(code);
                m_fut.setException(new DistributerException("failed to stat cluster tags for " + path, e));
            }
        }

        public Stat getStat() {
            try {
                return m_fut.get();
            } catch (InterruptedException e) {
                throw new DistributerException("interrupted while stating cluster tags");
            } catch (ExecutionException e) {
                DistributerException de = (DistributerException)e.getCause();
                throw de;
            }
        }
    }

    /**
     * @return a string tag that summarizes the zk versions of opmode and catalog
     */
    public String getClusterTag() {
        ClusterTagCallback forOpMode = new ClusterTagCallback();
        m_zk.exists(VoltZK.operationMode, false, forOpMode, null);
        Stat opModeStat = forOpMode.getStat();
        return String.valueOf((opModeStat != null ? opModeStat.getVersion() : 0));
    }

    /**
     * A wrapper around {@link ZooKeeper#setData(String, byte[], int, StatCallback, Object)} that
     * acts as its own invocation {@link AsyncCallback.StatCallback}
     */
    class SetData implements StatCallback {

        final String path;
        final int version;

        final Semaphore lock = new Semaphore(0);
        volatile Optional<Stat> stat = Optional.absent();
        volatile Optional<DistributerException> fault = Optional.absent();
        volatile Optional<Code> callbackCode = Optional.absent();

        SetData(String path, int version, byte [] data ) {
            this.path = path;
            this.version = version;
            m_zk.setData(path, data, version, this, null);
        }

        void internalProcessResult(int rc, String path, Object ctx, Stat stat) {
            callbackCode = Optional.of(Code.get(rc));
            Code code = callbackCode.get();
            if (code == Code.OK) {
                this.stat = Optional.of(stat);
            } else if (code == Code.NONODE || code == Code.BADVERSION) {
                // keep the fault but don't log it
                KeeperException e = KeeperException.create(code);
                fault = Optional.of(new DistributerException("failed to write to " + path, e));
            } else if (!m_done.get()) {
                fault = checkCode(code, "failed to write to %s", path);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            try {
                internalProcessResult(rc, path, ctx, stat);
            } finally {
                lock.release();
            }
        }

        public Stat getStat() {
            acquireAndRelease(lock);
            if (fault.isPresent()) throw fault.get();
            return stat.get();
        }

        public Code getCallbackCode() {
            acquireAndRelease(lock);
            return callbackCode.get();
        }
    }

    /**
     * An extension of {@link SetData} that is used to write to nodes their
     * assigned list of import channels. NB the mesh leader is the only one
     * that instantiates and uses this class
     */
    class SetNodeChannels extends SetData {

        SetNodeChannels(String path, int version, byte[] data) {
            super(path, version, data);
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            try {
                internalProcessResult(rc, path, ctx, stat);
            } finally {
                lock.release();
            }
        }
    }

    /**
     * A wrapper around {@link ZooKeeper#create(String, byte[], List, CreateMode, StringCallback, Object)}
     * that acts as its own invocation {@link AsyncCallback.StringCallback}
     */
    class CreateNode implements StringCallback {

        final Semaphore lock = new Semaphore(0);
        volatile Optional<String> node = Optional.absent();
        volatile Optional<DistributerException> fault = Optional.absent();

        CreateNode(String path, byte [] data, CreateMode cmode) {
            checkArgument(path != null && !path.trim().isEmpty(), "path is null or empty or blank");
            m_zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, cmode, this, null);
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            try {
                Code code = Code.get(rc);
                switch(code) {
                case NODEEXISTS:
                    code = Code.OK;
                    break;
                case OK:
                    node = Optional.of(name);
                    break;
                default:
                    node = Optional.of(path);
                }
                fault = checkCode(code, "cannot create node %s", node.get());
            } finally {
                lock.release();
            }
        }

        String getNode() {
            acquireAndRelease(lock);
            if (fault.isPresent()) throw fault.get();
            return node.get();
        }
    }

    /**
     * A wrapper around a more tolerant {@link ZooKeeper#delete(String, int, VoidCallback, Object)} that
     * acts as its own invocation {@link AsyncCallback.VoidCallback}.
     */
    class DeleteNode implements VoidCallback {
        final String path;

        final Semaphore lock = new Semaphore(0);
        volatile Optional<DistributerException> fault = Optional.absent();
        volatile Optional<Code> callbackCode = Optional.absent();

        DeleteNode(String path) {
            checkArgument(path != null && !path.trim().isEmpty(), "path is null or empty or blank");
            this.path = path;
            m_zk.delete(path, -1, this, null);
        }

        void internalProcessResult(int rc, String path, Object ctx) {
            callbackCode = Optional.of(Code.get(rc));
            switch (callbackCode.get()) {
            case OK:
            case NONODE:
            case SESSIONEXPIRED:
            case SESSIONMOVED:
            case CONNECTIONLOSS:
                break;
            default:
                fault = checkCode(callbackCode.get(), "failed to delete %s", path);
                break;
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            try {
                internalProcessResult(rc, path, ctx);
            } finally {
                lock.release();
            }
        }

        public Code getCallbackCode() {
            acquireAndRelease(lock);
            return callbackCode.get();
        }

        public void onComplete() {
            acquireAndRelease(lock);
            if (fault.isPresent()) throw fault.get();
        }
    }

    /**
     * A wrapper around {@link ZooKeeper#getChildren(String, Watcher, Children2Callback, Object)} that
     * acts as its own one time {@link Watcher} and its own {@link AsyncCallback.Children2Callback}
     */
    class GetChildren extends DistributerRunnable implements Children2Callback, Watcher {

        final String path;
        final Semaphore lock = new Semaphore(0);

        volatile Optional<Stat> stat = Optional.absent();
        volatile Optional<NavigableSet<String>> children = Optional.absent();
        volatile Optional<DistributerException> fault = Optional.absent();

        GetChildren(String path) {
            checkArgument(path != null && !path.trim().isEmpty(), "path is null or empty or blank");
            this.path = path;
            m_zk.getChildren(path, this, this, null);
        }

        void internalProcessResults(int rc, String path, Object ctx,
                List<String> children, Stat stat) {
            Code code = Code.get(rc);
            if (code == Code.OK) {
                NavigableSet<String> childset = ImmutableSortedSet.copyOf(children);
                this.stat = Optional.of(stat);
                this.children = Optional.of(childset);
            } else if (code == Code.SESSIONEXPIRED) {
                // keep the fault but don't log it
                KeeperException e = KeeperException.create(code);
                fault = Optional.of(new DistributerException("unable to get children for " + path, e));
            } else if (!m_done.get()) {
                fault = checkCode(code, "unable to get children for %s", path);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx,
                List<String> children, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, children, stat);
            } finally {
                lock.release();
            }
        }

        public NavigableSet<String> getChildren() {
            acquireAndRelease(lock);
            if (fault.isPresent()) throw fault.get();
            return children.get();
        }

        public Optional<Stat> getStat() {
            return stat;
        }

        @Override
        public void process(WatchedEvent e) {
            if (   e.getState() == KeeperState.SyncConnected
                && e.getType() == EventType.NodeChildrenChanged
                && !m_done.get())
            {
                m_es.submit(this);
            }
        }

        @Override
        public void susceptibleRun() throws Exception {
            new GetChildren(path);
        }
    }

    /**
     * An extension of {@link GetChildren} that is used to determine the distributer mesh leader
     */
    class ElectLeader extends GetChildren {
        final CreateNode leaderCandidate;

        ElectLeader(String path, CreateNode leaderCandidate) {
            super(path);
            this.leaderCandidate = Preconditions.checkNotNull(leaderCandidate,"candidate is null");
        }

        @Override
        public void processResult(int rc, String path, Object ctx,
                final List<String> children, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, children, stat);
                if (Code.get(rc) != Code.OK || m_done.get()) {
                    return;
                }
                m_es.submit(new DistributerRunnable() {
                    final int participants = children.size();
                    @Override
                    public void susceptibleRun() throws Exception {
                        String candidate = basename.apply(leaderCandidate.getNode());
                        if (!m_isLeader && candidate.equals(ElectLeader.this.children.get().first())) {
                            m_isLeader = true;
                            LOG.info("LEADER (" + m_hostId + ") is now the importer channel leader");
                            if (m_hosts.getReference().size() == participants) {
                                LOG.info(
                                        "(" + m_hostId
                                        + ") LEADER assign channels task triggered on on elector node change"
                                        );
                                new AssignChannels().run();
                            }
                        }
                    }
                });
            } finally {
                lock.release();
            }
        }

        boolean elect() {
            return getChildren().first().equals(leaderCandidate.getNode());
        }

        @Override
        public void susceptibleRun() throws Exception {
            ElectLeader ng = new ElectLeader(path, leaderCandidate);
            checkState(
                    ng.path.equals(path),
                    "mismatched paths on watcher resubmit: %s <> %s",
                    path, ng.path
                    );
        }
    }

    /**
     * A wrapper around {@link ZooKeeper#getData(String, Watcher, DataCallback, Object)} that acts
     * as its own {@link Watcher}, and {@link AsyncCallback.DataCallback}
     */
    class GetData extends DistributerRunnable implements DataCallback, Watcher {

        final String path;
        final Semaphore lock = new Semaphore(0);

        volatile Optional<Stat> stat = Optional.absent();
        volatile Optional<byte[]> data = Optional.absent();
        volatile Optional<DistributerException> fault = Optional.absent();

        protected GetData(final String path, final boolean doInvokeZookeeper) {
            checkArgument(path != null && !path.trim().isEmpty(), "path is null or empty or blank");
            this.path = path;
            if (doInvokeZookeeper) {
                invokeZookeeperGetData();
            }
        }

        public GetData(final String path) {
            this(path, true);
        }

        protected void invokeZookeeperGetData() {
            m_zk.getData(path, this, this, null);
        }

        @Override
        public void process(WatchedEvent e) {
            if (   e.getState() == KeeperState.SyncConnected
                && e.getType() == EventType.NodeDataChanged
                && !m_done.get())
            {
                m_es.submit(this);
            }
        }

        void internalProcessResults(int rc, String path, Object ctx, byte[] data, Stat stat) {
            Code code = Code.get(rc);
            if (code == Code.OK) {
                this.stat = Optional.of(stat);
                this.data = Optional.of(data != null && data.length > 0 ? data : EMPTY_ARRAY);
            } else if (code == Code.NONODE || code == Code.SESSIONEXPIRED || m_done.get()) {
                // keep the fault but don't log it
                KeeperException e = KeeperException.create(code);
                fault = Optional.of(new DistributerException(path + " went away", e));
            } else {
                fault = checkCode(code, "unable to read data in %s", path);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, data, stat);
            } finally {
                lock.release();
            }
        }

        @Override
        public void susceptibleRun() throws Exception {
            GetData ng = new GetData(path);
            checkState(
                    ng.path.equals(path),
                    "mismatched paths on watcher resubmit: %s <> %s",
                    path, ng.path
                    );
        }

        public byte [] getData() {
            acquireAndRelease(lock);
            if (fault.isPresent()) throw fault.get();
            return data.get();
        }
    }

    /**
     * An extension of {@link GetData} that reads the date contents of a host
     * node as set of assigned importer channels. It merges the set against the
     * the import channels in {@link ChannelDistributer#m_specs}, and if the host
     * node is its own, it sends to the notification queue instances of
     * {@link ChannelAssignment} that describe assignment changes.
     *
     * It is instantiated mainly in {@link MonitorHostNodes}
     *
     */
    class GetHostChannels extends GetData {
        volatile Optional<NavigableSet<ChannelSpec>> nodespecs;

        final String host;

        public GetHostChannels(final String path) {
            super(path,false);
            this.host = basename.apply(path);
            checkArgument(
                    this.host != null && !this.host.trim().isEmpty(),
                    "path has undiscernable basename: %s", path
                    );
            nodespecs = Optional.absent();
            invokeZookeeperGetData();
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, data, stat);
                if (Code.get(rc) != Code.OK) {
                    return;
                }
                NavigableSet<ChannelSpec> nspecs;
                try {
                    nspecs = asChannelSet(data);
                } catch (IllegalArgumentException|JSONException e) {
                    fault = Optional.of(
                            loggedDistributerException(e, "failed to parse json in %s", path)
                            );
                    return;
                }
                nodespecs = Optional.of(nspecs);

                final String hval = basename.apply(path);
                if (hval == null || hval.trim().isEmpty()) {
                    IllegalArgumentException e = new IllegalArgumentException(
                            "path has undiscernable basename: \"" + path + "\""
                            );
                    fault = Optional.of(
                            loggedDistributerException(e, "could not derive host from %s", path)
                            );
                    return;
                }

                Predicate<Map.Entry<ChannelSpec,String>> inSpecs =
                        ChannelSpec.specKeyIn(nspecs, String.class);
                Predicate<Map.Entry<ChannelSpec,String>> thisHost =
                        hostValueIs(hval, ChannelSpec.class);

                int [] sstamp = new int[]{0};
                AtomicInteger dstamp = m_hosts.getReference().get(hval);
                if (dstamp == null) {
                    LOG.warn("(" + m_hostId + ") has no data stamp for "
                            + hval + ", host registry contains: " + m_hosts.getReference()
                            );
                    dstamp = new AtomicInteger(0);
                }
                NavigableMap<ChannelSpec,String> prev = null;
                NavigableSet<ChannelSpec> oldspecs = null;
                ImmutableSortedMap.Builder<ChannelSpec,String> mbldr = null;

                do {
                    final int specversion = dstamp.get();
                    // callback has a stale version
                    if (specversion >= stat.getVersion()) {
                        return;
                    }
                    // register the data node version
                    if (!dstamp.compareAndSet(specversion, stat.getVersion())) {
                        return;
                    }

                    prev = m_specs.get(sstamp);
                    oldspecs = Maps.filterEntries(prev, thisHost).navigableKeySet();
                    // rebuild the assigned channel spec list
                    mbldr = ImmutableSortedMap.naturalOrder();
                    mbldr.putAll(Maps.filterEntries(prev, not(or(thisHost, inSpecs))));
                    for (ChannelSpec spec: nspecs) {
                        mbldr.put(spec, hval);
                    }

                } while (!m_specs.compareAndSet(prev, mbldr.build(), sstamp[0], sstamp[0]+1));

                if (hval.equals(m_hostId) && !m_done.get()) {
                    ChannelAssignment assignment = new ChannelAssignment(
                            oldspecs, nspecs, stat.getVersion()
                            );
                    for (ImporterChannelAssignment cassigns: assignment.getImporterChannelAssignments()) {
                        if (m_done.get()) break;
                        m_eb.post(cassigns);
                    }
                    if (!assignment.getRemoved().isEmpty()) {
                        LOG.info("(" + m_hostId + ") removing the following channel assignments: " + assignment.getRemoved());
                    }
                    if (!assignment.getAdded().isEmpty()) {
                        LOG.info("(" + m_hostId + ") adding the following channel assignments: " + assignment.getAdded());
                    }
                }
            } finally {
                lock.release();
            }
        }

        @Override
        public void susceptibleRun() throws Exception {
            new GetHostChannels(path);
        }

        NavigableSet<ChannelSpec> getSpecs() {
            acquireAndRelease(lock);
            if (fault.isPresent()) throw fault.get();
            return nodespecs.get();
        }
    }

    /**
     * An extension of {@link GetData} that monitors the content of the registered
     * importer channel list.
     */
    class GetChannels extends GetData {

        volatile Optional<NavigableSet<ChannelSpec>> channels;

        public GetChannels(String path) {
            super(path, false);
            channels = Optional.absent();
            invokeZookeeperGetData();
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] nodeData, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, nodeData, stat);
                if (Code.get(rc) != Code.OK) {
                    return;
                }
                try {
                    channels = Optional.of(asChannelSet(data.get()));
                } catch (IllegalArgumentException|JSONException e) {
                    fault = Optional.of(
                            loggedDistributerException(e, "failed to parse json in %s", path)
                            );
                    return;
                }

                int [] stamp = new int[]{0};
                NavigableSet<ChannelSpec> oldspecs = m_channels.get(stamp);
                //If I have newer version dont process.
                if (stamp[0] > stat.getVersion()) {
                    return;
                }
                if (!m_channels.compareAndSet(oldspecs, channels.get(), stamp[0], stat.getVersion())) {
                    return;
                }
                LOG.info("(" + m_hostId + ") successfully received channel assignment master copy");
                if (m_isLeader && !m_done.get()) {
                    LOG.info(
                            "(" + m_hostId
                            + ") LEADER assign channels task triggered on changed master copy receipt"
                            );
                    m_es.submit(new AssignChannels());
                }
            } finally {
                lock.release();
            }
        }

        @Override
        public void susceptibleRun() throws Exception {
            new GetChannels(path);
        }

        NavigableSet<ChannelSpec> getChannels() {
            acquireAndRelease(lock);
            if (fault.isPresent()) {
                throw fault.get();
            }
            return channels.get();
        }
    }

    /**
     * An extension of {@link GetData} that monitors the content of the cluster
     * operational mode
     */
    class GetOperationMode extends GetData {
        volatile Optional<VersionedOperationMode> opmode = Optional.absent();

        GetOperationMode(String path) {
            super(path,false);
            opmode = Optional.absent();
            invokeZookeeperGetData();
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] nodeData, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, nodeData, stat);
                if (Code.get(rc) != Code.OK) {
                    return;
                }
                OperationMode next = VoltDB.instance().getStartMode();
                if (nodeData != null && nodeData.length > 0) try {
                    next = OperationMode.valueOf(nodeData);
                } catch (IllegalArgumentException e) {
                    fault = Optional.of(loggedDistributerException(
                            e, "unable to decode content in operation node: \"%s\"",
                            new String(nodeData, StandardCharsets.UTF_8)
                            ));
                    return;
                }
                opmode = Optional.of(new VersionedOperationMode(next, stat.getVersion()));

                int [] stamp = new int[]{0};
                OperationMode prev = m_mode.get(stamp);
                if (stamp[0] > stat.getVersion()) {
                    opmode = Optional.of(new VersionedOperationMode(prev, stamp[0]));
                    return;
                }
                if (!m_mode.compareAndSet(prev, next, stamp[0], stat.getVersion())) {
                    return;
                }
                if (prev == next) {
                    return;
                }
                if (m_isLeader && !m_done.get() && next == OperationMode.RUNNING) {
                    LOG.info(
                            "(" + m_hostId
                            + ") LEADER assign channels task triggered on cluster state change"
                            );
                    m_es.submit(new AssignChannels());
                }
                m_eb.post(opmode.get());

            } finally {
                lock.release();
            }
        }

        @Override
        public void susceptibleRun() throws Exception {
            new GetOperationMode(path);
        }

        VersionedOperationMode getMode() {
            acquireAndRelease(lock);
            if (fault.isPresent()) {
                throw fault.get();
            }
            if (!opmode.isPresent()) {
                throw new DistributerException("failed to mirror cluster operation mode");
            }
            return opmode.get();
        }
    }

    /**
     * An extension of {@link GetChildren} that monitor hosts that participate in the distributer mesh.
     * if any nodes leave the mesh, then they are removed from the assigned list, if any are added
     * then their channel assignments will start to get monitored.
     */
    class MonitorHostNodes extends GetChildren {

        MonitorHostNodes(String path) {
            super(path);
        }

        @Override
        public void processResult(int rc, String path, Object ctx,
                List<String> children, Stat stat) {
            try {
                internalProcessResults(rc, path, ctx, children, stat);
                if (Code.get(rc) != Code.OK) {
                    return;
                }

                int [] hstamp = new int[]{0};
                NavigableMap<String,AtomicInteger> oldgen = m_hosts.get(hstamp);
                if (hstamp[0] >= stat.getCversion()) {
                    return;
                }

                final Set<String> added   = Sets.difference(this.children.get(), oldgen.navigableKeySet());
                final Set<String> removed = Sets.difference(oldgen.navigableKeySet(), this.children.get());

                ImmutableSortedMap.Builder<String,AtomicInteger> hbldr = ImmutableSortedMap.naturalOrder();
                hbldr.putAll(Maps.filterEntries(oldgen, not(hostKeyIn(removed, AtomicInteger.class))));
                for (String add: added) {
                    hbldr.put(add, new AtomicInteger(0));
                }
                NavigableMap<String,AtomicInteger> newgen = hbldr.build();

                if (!m_hosts.compareAndSet(oldgen, newgen, hstamp[0], stat.getCversion())) {
                    return;
                }

                if (!removed.isEmpty()) {
                    final Predicate<Map.Entry<ChannelSpec,String>> inRemoved =
                            hostValueIn(removed, ChannelSpec.class);

                    int [] sstamp = new int[]{0};
                    NavigableMap<ChannelSpec,String> prev = null;
                    NavigableMap<ChannelSpec,String> next = null;

                    do {
                        prev = m_specs.get(sstamp);
                        next = Maps.filterEntries(prev, not(inRemoved));
                    } while (!m_specs.compareAndSet(prev, next, sstamp[0], sstamp[0]+1));

                    LOG.info("(" + m_hostId + ") host(s) " + removed + " no longer servicing importer channels");

                    if (m_isLeader && !m_done.get()) {
                        LOG.info(
                                "(" + m_hostId
                                + ") LEADER assign channels task triggered on node removal"
                                );
                        m_es.submit(new AssignChannels());
                    }
                }

                if (!added.isEmpty() && !m_done.get()) {
                    m_es.submit(new DistributerRunnable() {
                        @Override
                        public void susceptibleRun() throws Exception {
                            for (String host: added) {
                                LOG.info("(" + m_hostId + ") starting to monitor host node " + host);
                                new GetHostChannels(joinZKPath(HOST_DN, host));
                            }
                        }
                    });
                }
            } finally {
                lock.release();
            }
        }

        @Override
        public void susceptibleRun() throws Exception {
            new MonitorHostNodes(path);
        }
    }

    // a form of type alias
    final static class HostsRef extends AtomicStampedReference<NavigableMap<String,AtomicInteger>> {
        static final NavigableMap<String,AtomicInteger> EMPTY_MAP = ImmutableSortedMap.of();

        public HostsRef(NavigableMap<String,AtomicInteger> initialRef, int initialStamp) {
            super(initialRef, initialStamp);
        }

        public HostsRef() {
            this(EMPTY_MAP, 0);
        }
    }

    // a form of type alias
    final static class ChannelsRef extends AtomicStampedReference<NavigableSet<ChannelSpec>> {
        static final NavigableSet<ChannelSpec> EMPTY_SET = ImmutableSortedSet.of();

        public ChannelsRef(NavigableSet<ChannelSpec> initialRef, int initialStamp) {
            super(initialRef, initialStamp);
        }

        public ChannelsRef() {
            this(EMPTY_SET, 0);
        }
    }

    // a form of type alias
    final static class UnregisteredRef extends AtomicStampedReference<NavigableSet<String>> {
        static final NavigableSet<String> EMPTY_SET = ImmutableSortedSet.of();

        public UnregisteredRef(NavigableSet<String> initialRef, int initialStamp) {
            super(initialRef, initialStamp);
        }

        public UnregisteredRef() {
            this(EMPTY_SET, 0);
        }
    }

    // a form of type alias
    final static class SpecsRef extends AtomicStampedReference<NavigableMap<ChannelSpec,String>> {
        static final NavigableMap<ChannelSpec,String> EMPTY_MAP = ImmutableSortedMap.of();

        public SpecsRef(NavigableMap<ChannelSpec,String> initialRef, int initialStamp) {
            super(initialRef, initialStamp);
        }

        public SpecsRef() {
            this(EMPTY_MAP, 0);
        }
    }

    // a form of type alias
    final static class CallbacksRef
        extends AtomicStampedReference<NavigableMap<String,ChannelChangeCallback>> {

        static final NavigableMap<String,ChannelChangeCallback> EMTPY_MAP =
                ImmutableSortedMap.of();

        public CallbacksRef(
                NavigableMap<String,ChannelChangeCallback> initialRef,
                int initialStamp) {
            super(initialRef, initialStamp);
        }

        public CallbacksRef() {
            this(EMTPY_MAP,0);
        }
    }

    static <K> Predicate<Map.Entry<K, String>> hostValueIs(final String s, Class<K> clazz) {
        return new Predicate<Map.Entry<K,String>>() {
            @Override
            public boolean apply(Entry<K, String> e) {
                return s.equals(e.getValue());
            }
            @Override
            public String toString() {
                return "Predicate.hostValueIs[Map.Entry.getValue() is \"" + s + "\" ]";
            }
        };
    }

    static <K> Predicate<Map.Entry<K, String>> hostValueIn(final Set<String> s, Class<K> clazz) {
        return new Predicate<Map.Entry<K,String>>() {
            @Override
            public boolean apply(Entry<K, String> e) {
                return s.contains(e.getValue());
            }
        };
    }

    static <V> Predicate<Map.Entry<String,V>> hostKeyIn(final Set<String> s, Class<V> clazz) {
        return new Predicate<Map.Entry<String,V>>() {
            @Override
            public boolean apply(Entry<String,V> e) {
                return s.contains(e.getKey());
            }
        };
    }

    final static Function<String,String> basename = new Function<String, String>() {
        @Override
        public String apply(String path) {
            return new File(path).getName();
        }
    };

    public final static Predicate<ImporterChannelAssignment> importerIs(final String importer) {
        return new Predicate<ImporterChannelAssignment>() {
            @Override
            public boolean apply(ImporterChannelAssignment assignment) {
                return importer.equals(assignment.getImporter());
            }
        };
    }

    public final static Predicate<ImporterChannelAssignment> importerIn(final Set<String> importers) {
        return new Predicate<ImporterChannelAssignment>() {
            @Override
            public boolean apply(ImporterChannelAssignment assignment) {
                return importers.contains(assignment.getImporter());
            }
        };
    }

    public final static <T> Predicate<T> in(final Set<T> set) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T m) {
                return set.contains(m);
            }
        };
    }
}
