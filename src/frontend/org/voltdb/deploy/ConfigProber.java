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
import static com.google_voltpatches.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.StartAction;
import org.voltdb.StatusTracker;
import org.voltdb.utils.HTTPAdminListener;

import com.google_voltpatches.common.collect.BiMap;
import com.google_voltpatches.common.collect.HashBiMap;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

public class ConfigProber implements Closeable {

    private final static VoltLogger LOG = new VoltLogger("HOST");

    public static enum Endpoint {
        STATUS("/management/status"),
        CONFIG("/management/config"),
        KSAFETY("/management/ksafety");

        final static Map<String, Endpoint> s_paths = ImmutableMap.of(
                STATUS.path(),  STATUS,
                CONFIG.path(),  CONFIG,
                KSAFETY.path(), KSAFETY);

        final String m_path;

        Endpoint(String path) {
            m_path = path;
        }

        public String path() {
            return m_path;
        }

        public static Endpoint valueOf(URI uri) {
            return s_paths.get(uri.getPath());
        }
    }

    /**
     * Boiler plate method to log an error message and wrap, and return a {@link ConfigProberException}
     * around the message and cause
     *
     * @param cause fault origin {@link Throwable}
     * @param format a {@link String#format(String, Object...) compliant format string
     * @param args formatter arguments
     * @return a {@link ConfigProberException}
     */
    protected static ConfigProberException loggedProberException(Throwable cause, String format, Object...args) {
        Optional<ConfigProberException> causeFor = ConfigProberException.isCauseFor(cause);
        if (causeFor.isPresent()) {
            return causeFor.get();
        }
        String msg = String.format(format, args);
        if (cause != null) {
            LOG.error(msg, cause);
            return new ConfigProberException(msg, cause);
        } else {
            LOG.error(msg);
            return new ConfigProberException(msg);
        }
    }

    public static Optional<IOException> IOExceptionInCauseChain(Throwable cause) {
        Optional<IOException> ioe = Optional.empty();
        while (cause != null && !ioe.isPresent()) {
            if (cause instanceof IOException) {
                ioe = Optional.of((IOException)cause);
            }
            cause = cause.getCause();
        }
        return ioe;
    }

   /**
     * Base class for all runnables submitted to the executor service
     */
    protected abstract class ProbeRunnable implements Runnable {
        @Override
        public void run() {
            try {
                if (!m_done.get()) {
                    susceptibleRun();
                }
            } catch (Exception ex) {
                throw loggedProberException(ex, "Fault occured while executing runnable");
            }
        }

        public abstract void susceptibleRun() throws Exception;
    }

    protected final ConfigProbeTracker m_tracker;
    protected final Set<URI> m_configEndpoints;
    protected final Set<URI> m_statusEndpoints;
    protected final HttpAsyncClientBuilder m_clientBuilder;
    protected final NameValuePair m_startNV;
    protected final Supplier<CloseableHttpAsyncClient> m_client;
    protected final ObjectReader m_configReader;
    protected final ObjectReader m_statusReader;
    protected final ObjectReader m_ksafetyReader;
    protected final ObjectWriter m_allClearWriter;
    protected final AtomicBoolean m_done;
    protected final BlockingDeque<RequestPair> m_deque;
    protected final Map<UUID, ConfigProbeResponse> m_responses;
    protected final Map<UUID, StatusTracker> m_statuses;
    protected final Map<UUID, KSafetyResponse> m_lacks;
    protected final BiMap<UUID,String> m_alive;
    protected final ScheduledThreadPoolExecutor m_es;
    protected final int m_safety;
    protected final int m_hostCount;
    protected final String m_clusterName;
    protected final boolean m_commandLogEnabled;
    protected final UUID m_self;

    private CloseableHttpAsyncClient m_asyncClient = null;

    public ConfigProber(
            String clusterName,
            Set<String> endpoints,
            int kSafety,
            boolean isCommandLogEnabled,
            ConfigProbeTracker tracker
    ) {
        checkArgument(clusterName != null && !clusterName.trim().isEmpty(),
                "cluster name is null, empty, or blank");
        checkArgument(tracker != null, "config log tracker is null");
        checkArgument(endpoints != null && !endpoints.isEmpty(), "endpoints is null or empty");
        checkArgument(kSafety >= 0, "ksafety %s is negative", kSafety);

        m_clusterName = clusterName;
        m_safety = kSafety + 1;
        m_commandLogEnabled = isCommandLogEnabled;
        m_tracker = tracker;
        m_hostCount = endpoints.size();
        m_self = tracker.getInitialConfigProbeResponse().getStartUuid();
        m_startNV = new BasicNameValuePair("start-uuid", m_self.toString());

        ImmutableSet.Builder<URI> csbld = ImmutableSet.builder();
        ImmutableSet.Builder<URI> ssbld = ImmutableSet.builder();
        for (String ep: endpoints) {
            checkArgument(ep != null && !ep.trim().isEmpty(), "endpoint in set is null, empty, or blank");
            ssbld.add(URI.create("http://" + ep + Endpoint.STATUS.path()));
            csbld.add(URI.create("http://" + ep + Endpoint.CONFIG.path()));
        }
        m_configEndpoints = csbld.build();
        m_statusEndpoints = ssbld.build();

        ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            throw new ConfigProberException("Unable to setup the http connection io reactor", e);
        }
        PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
        connManager.setMaxTotal(m_configEndpoints.size()+2);
        connManager.setDefaultMaxPerRoute(m_configEndpoints.size()+2);

        m_clientBuilder = HttpAsyncClients.custom().setConnectionManager(connManager);
        m_client = new Supplier<CloseableHttpAsyncClient>() {
            @Override
            public CloseableHttpAsyncClient get() {
                if (m_asyncClient == null || !m_asyncClient.isRunning()) {
                    m_asyncClient = m_clientBuilder.build();
                    m_asyncClient.start();
                }
                return m_asyncClient;
            }
        };
        m_configReader = HTTPAdminListener.MapperHolder.mapper.reader(ConfigProbeResponse.class);
        m_statusReader = HTTPAdminListener.MapperHolder.mapper.reader(StatusTracker.class);
        m_ksafetyReader = HTTPAdminListener.MapperHolder.mapper.reader(KSafetyResponse.class);
        m_allClearWriter = HTTPAdminListener.MapperHolder.mapper.writerWithType(LeaderAllClear.class);
        m_done = new AtomicBoolean(false);
        m_deque = new LinkedBlockingDeque<>();
        m_responses = new LinkedHashMap<>();
        m_statuses = new LinkedHashMap<>();
        m_lacks = new LinkedHashMap<>();
        m_alive = HashBiMap.create();
        m_es = CoreUtils.getScheduledThreadPoolExecutor("Config Prober Retry Worker", 1, CoreUtils.SMALL_STACK_SIZE);
    }

    protected String getRequestQuery() {
        NameValuePair meshTimeoutNV =  new BasicNameValuePair("mesh-timeout", Long.toString(m_tracker.getMeshTimeout()));
        return URLEncodedUtils.format(ImmutableList.of(m_startNV, meshTimeoutNV), StandardCharsets.UTF_8);
    }

    protected HttpGet asGET(URI base) {
        URI rquri = null;
        try {
            rquri = new URI(base.getScheme(),base.getAuthority(),base.getPath(),getRequestQuery(),base.getFragment());
        } catch (URISyntaxException e) {
            throw loggedProberException(e, "failed to build request uri for %s", base);
        }
        return new HttpGet(rquri);
    }

    protected static HttpGet asEndpointGet(Endpoint ep, String authority) {
        URI rquri = null;
        try {
            rquri = new URI("http", authority, ep.path(), null, null);
        } catch (URISyntaxException e) {
            throw loggedProberException(e, "failed to build request uri for %s and %s", authority, ep.path());
        }
        return new HttpGet(rquri);
    }

    protected HttpPut asLeaderAllClear(String authority, LeaderAllClear ac) {
        URI rquri = null;
        try {
            rquri = new URI("http", authority, Endpoint.CONFIG.path() + "/", null, null);
        } catch (URISyntaxException e) {
            throw loggedProberException(e, "failed to build request uri for %s and %s", authority, Endpoint.CONFIG.path());
        }
        String jsn = null;
        try {
            jsn = m_allClearWriter.writeValueAsString(ac);
        } catch (IOException e) {
            throw loggedProberException(e, "failed to serialize %s", ac);
        }
        HttpPut put = new HttpPut(rquri);
        put.setEntity(new StringEntity(jsn, ContentType.APPLICATION_JSON));
        return put;
    }

    protected boolean needsMoreResponses() {
        int unmeshed = 0, meshed = 0;
        for (StatusTracker st: m_statuses.values()) {
            if (st.getNodeState().meshed()) ++meshed;
            if (st.getNodeState().unmeshed()) ++unmeshed;
        }
        // if (kfactor + 1) nodes are unmeshed it means that every member in the cluster
        // is starting.
        if (unmeshed >= m_safety) {
            return m_responses.size() < m_hostCount || m_statuses.size() < m_hostCount;
        }
        return meshed == 0
            || m_responses.size() < 2
            || m_statuses.size() < 2
            || !m_responses.containsKey(m_self);
    }

    public static class Result {

        protected final StartAction startAction;
        protected final String leader;
        protected final boolean adminMode;
        protected final int hostCount;

        protected Result(StartAction startAction, String leader, boolean adminMode, int hostCount) {
            this.startAction = startAction;
            this.leader = leader;
            this.adminMode = adminMode;
            this.hostCount = hostCount;
        }

        public StartAction getStartAction() {
            return startAction;
        }

        public String getLeader() {
            return leader;
        }

        public boolean isAdminMode() {
            return adminMode;
        }

        public int getHostCount() {
            return hostCount;
        }

        @Override
        public String toString() {
            return "Result [startAction=" + startAction + ", leader=" + leader
                    + ", adminMode=" + adminMode + ", hostCount=" + hostCount
                    + "]";
        }
    }

    protected RequestPair receiveAndProcessResponse() {
        RequestPair rp = null;
        try {
            while (rp == null) {
                rp = m_deque.poll(m_tracker.getMillisToTimeout(), TimeUnit.MILLISECONDS);
                if (rp == null && m_tracker.getMillisToTimeout() == 0) {
                    throw loggedProberException(null, "could not complete the configuration probe in the alloted time");
                } else if (rp == null) {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            throw loggedProberException(e, "interrupted while reading probe responses");
        }
        URI rquri = rp.m_request.getURI();
        HttpResponse rsp;
        try {
            rsp = rp.getFuture().get();
        } catch (InterruptedException e) {
            throw loggedProberException(e, "interrupted while reading probe responses");
        } catch (ExecutionException e) {
            throw loggedProberException(e.getCause(), "request to %s failed", rquri);
        }
        String msg;
        try {
            msg = EntityUtils.toString(rsp.getEntity());
        } catch (ParseException | IOException e) {
            throw loggedProberException(e, "unable to parse response to request made to %s", rquri);
        }
        if (rsp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            LOG.error("Request to " + rquri +" failed with " + rsp.getStatusLine().getReasonPhrase() + ", message:\n" + msg);
            throw new ConfigProberException("Request to %s failed with %s", rquri, rsp.getStatusLine().getReasonPhrase());
        }
        switch(Endpoint.valueOf(rquri)) {
        case CONFIG:
            ConfigProbeResponse probeResponse;
            try {
                probeResponse = m_configReader.readValue(msg);
            } catch (IOException e) {
                throw loggedProberException(e, "content from request to %s is not a config probe response: \n%s", rquri, msg);
            }
            m_responses.put(probeResponse.getStartUuid(), probeResponse);
            m_alive.put(probeResponse.getStartUuid(), rquri.getAuthority());
            break;
        case STATUS:
            StatusTracker statusResponse;
            try {
                statusResponse = m_statusReader.readValue(msg);
            } catch (IOException e) {
                throw loggedProberException(e, "content from request to %s is not a status tracker: \n%s", rquri, msg);
            }
            m_statuses.put(statusResponse.getStartUuid(), statusResponse);
            m_alive.put(statusResponse.getStartUuid(),rquri.getAuthority());
            break;
        case KSAFETY:
            KSafetyResponse ksafetyResponse;
            try {
                ksafetyResponse = m_ksafetyReader.readValue(msg);
            } catch (IOException e) {
                throw loggedProberException(e, "content from request to %s is not a ksafety response: \n%s", rquri, msg);
            }
            m_lacks.put(ksafetyResponse.getStartUuid(), ksafetyResponse);
            break;
        default:
            break;
        }
        return rp;
    }

    public Result probe() {
        checkState(!m_done.get(), "probe is stale");

        for (URI cep: m_configEndpoints) {
            HttpGet getConfig = asGET(cep);
            m_client.get().execute(getConfig, new RequestPair(getConfig));
        }
        for (URI sep: m_statusEndpoints) {
            HttpGet getStatus = asGET(sep);
            m_client.get().execute(getStatus, new RequestPair(getStatus));
        }
        while (needsMoreResponses()) {
            receiveAndProcessResponse();
        }

        UUID conf = m_tracker.getInitialConfigProbeResponse().getConfigHash();

        for (StatusTracker st: m_statuses.values()) {
            if (!m_clusterName.equals(st.getClusterName())) {
                throw loggedProberException(null, "received mismatched cluster names");
            }
        }
        for (ConfigProbeResponse cpr: m_responses.values()) {
            if (!conf.equals(cpr.configHash)) {
                throw loggedProberException(null, "probed nodes have mismatching configurations");
            }
        }
        if (!m_alive.containsKey(m_self)) {
            throw loggedProberException(null, "this node is not part of the configured mesh");
        }
        return waitForExpectedProbes();
    }

    protected Result waitForExpectedProbes() {
        int unmeshed = 0, operational = 0;
        for (StatusTracker st: m_statuses.values()) {
            if (st.getNodeState().unmeshed()) ++unmeshed;
            if (st.getNodeState().operational()) ++operational;
        }
        // if (kfactor + 1) nodes are unmeshed it means that every member in the cluster
        // is starting. This requires more stringent checks
        //    1) this node has to recieve probes from all the members
        //    2) the mesh configuration must match in all
        if (unmeshed >= m_safety) {
            Map<UUID,Long> tracked;
            try {
                tracked = m_tracker.waitForExpectedProbes();
            } catch (InterruptedException e) {
                throw loggedProberException(e, "interrupted while waiting to receive probes");
            } catch (TimeoutException e) {
                throw loggedProberException(null, "could not complete the configuration probe in the alloted time");
            }
            if (tracked.size() != m_responses.size() || !tracked.keySet().containsAll(m_responses.keySet())) {
                throw loggedProberException(null, "received probes sources don't match the destinations of the probes sent out by this node");
            }
            UUID mesh = m_tracker.getInitialConfigProbeResponse().getMeshHash();
            for (ConfigProbeResponse cpr: m_responses.values()) {
                if (!mesh.equals(cpr.getMeshHash())) {
                    throw loggedProberException(null, "probed members have mismatching participating nodes configuration");
                }
            }

            StartAction startAction = m_commandLogEnabled ? StartAction.RECOVER : StartAction.CREATE;

            // wait for the leader to send out the all clear
            UUID leader = pickLeader();
            if (!m_self.equals(leader)) {
                LeaderAllClear allClear;
                try {
                    allClear = m_tracker.waitForLeaderAllClear();
                } catch (InterruptedException e) {
                    throw loggedProberException(e, "interrupted while waiting for leader all clear");
                } catch (TimeoutException e) {
                    throw loggedProberException(null, "could not complete the configuration probe in the alloted time");
                }
                if (!leader.equals(allClear.getStartUuid())) {
                    throw loggedProberException(null, "Reeived an all clear from an unexpected leader");
                }
            } else {
                sendLeaderAllClear();
            }
            return new Result(
                    startAction,
                    formatLeader(leader),
                    m_responses.values().stream().anyMatch(e->e.isAdmin()),
                    tracked.size());
        }

        if (operational == 0) {
            throw loggedProberException(null, "found a custer in impossible state: operational %d, unmeshed %d", operational, unmeshed);
        }
        // get the start UUIDs of at most two operational nodes
        // find their respective host and ports specification
        m_statuses.entrySet().stream()
                .filter(e -> e.getValue().getNodeState().operational() && m_alive.containsKey(e.getKey()))
                .limit(2)
                .map(e -> m_alive.get(e.getKey()))
                .map(s -> asEndpointGet(Endpoint.KSAFETY, s))
                .forEach(get -> m_client.get().execute(get, new RequestPair(get)));
        // wait until there is a KSafetyResponse
        while (m_lacks.isEmpty()) {
            receiveAndProcessResponse();
        }
        // if there is no partition that is lacking a replica
        KSafetyResponse ksr = m_lacks.values().iterator().next();
        StartAction startAction = StartAction.LIVE_REJOIN;
        int hostCount = ksr.getHosts();
        if (ksr.getLack() == 0) {
            startAction = StartAction.JOIN;
            hostCount += m_safety;
        }
        return new Result(
                startAction,
                formatLeader(pickLeader()),
                m_responses.values().stream().anyMatch(e->e.isAdmin()),
                hostCount);
    }

    protected UUID pickLeader() {
        return m_statuses.entrySet().stream()
                .filter(e -> e.getValue().getNodeState().operational() && m_responses.containsKey(e.getKey()))
                .map(Map.Entry::getKey).findFirst().orElse(ImmutableSortedSet.copyOf(m_alive.keySet()).first());
    }

    protected String formatLeader(UUID pick) {
        ConfigProbeResponse leaderConfig = m_responses.get(pick);
        HostAndPort leader = HostAndPort.fromString(leaderConfig.getInternalInterface());
        if (leader.getHostText().isEmpty()) {
            HostAndPort alive = HostAndPort.fromString(m_alive.get(pick));
            leader = HostAndPort.fromParts(alive.getHostText(), leader.getPort());
        }
        return leader.toString();
    }

    protected void sendLeaderAllClear() {
        final Map<UUID,String> alive = ImmutableMap.copyOf(m_alive);
        final ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor();
        } catch (IOReactorException e) {
            throw new ConfigProberException("Unable to setup the http connection io reactor for the all clear sender", e);
        }

        Runnable r = new Runnable() {
            @Override
            public void run() {
                PoolingNHttpClientConnectionManager connManager =
                        new PoolingNHttpClientConnectionManager(ioReactor);
                connManager.setMaxTotal(m_configEndpoints.size());
                connManager.setDefaultMaxPerRoute(m_configEndpoints.size());

                try (final CloseableHttpAsyncClient client = m_clientBuilder
                        .setConnectionManager(connManager).build()) {
                    client.start();
                    LeaderAllClear allClear = new LeaderAllClear(m_self);
                    List<Future<HttpResponse>> futs = alive.entrySet().stream()
                            .filter(e -> !m_self.equals(e.getKey()))
                            .map(e -> e.getValue())
                            .map(u -> client.execute(asLeaderAllClear(u, allClear), null))
                            .collect(Collectors.toList());
                    for (Future<HttpResponse> fut: futs) {
                        HttpResponse rsp;
                        try {
                            rsp = fut.get();
                        } catch (InterruptedException e) {
                            loggedProberException(e, "interrupted while waiting for leader all clear responses");
                            continue;
                        } catch (ExecutionException e) {
                            loggedProberException(e.getCause(), "leader all clear request failed");
                            continue;
                        }
                        String msg;
                        try {
                            msg = EntityUtils.toString(rsp.getEntity());
                        } catch (ParseException | IOException e) {
                            loggedProberException(e, "failed to decode leader all clear response");
                            continue;
                        }
                        if (rsp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                            LOG.error("Request to leader all clear failed with " + rsp.getStatusLine().getReasonPhrase() + ", message:\n" + msg);
                            continue;
                        }
                    }
                } catch (IOException e) {
                    LOG.error("failed to send leader all clear requests", e);
                }
            }
        };

        Thread sender = new Thread(r);
        sender.setName("Configuration Prober - Leader All Clear Sender");
        sender.setDaemon(true);
        sender.start();
    }

    @Override
    public void close() {
        if (m_done.compareAndSet(false, true)) {
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw loggedProberException(e, "interrupted while waiting for executor service termination");
            }
            try {
                m_client.get().close();
            } catch (IOException ignoreIt) {
            }
            m_deque.clear();
            m_responses.clear();
            m_statuses.clear();
            m_lacks.clear();
            m_alive.clear();
        }
    }

    public class RequestPair extends ProbeRunnable implements FutureCallback<HttpResponse> {
        private final HttpGet m_request;
        private final SettableFuture<HttpResponse> m_fut;

        public RequestPair(HttpGet request) {
            m_request = request;
            m_fut = SettableFuture.create();
        }

        @Override
        public void completed(HttpResponse result) {
            if (m_done.get()) return;
            m_fut.set(result);
            m_deque.offer(this);
        }

        @Override
        public void failed(Exception ex) {
            if (m_done.get()) return;
            Optional<IOException> ioe = IOExceptionInCauseChain(ex);
            if (ioe.isPresent()) {
                LOG.rateLimitedLog(30, Level.INFO, ex,
                        "Probe request to %s failed. Retrying in 5 seconds",
                        m_request.getURI());
                m_es.schedule(this, 5, TimeUnit.SECONDS);
            } else {
                m_fut.setException(ex);
                m_deque.offer(this);
            }
        }

        @Override
        public void cancelled() {
            if (m_done.get()) return;
            m_fut.setException(loggedProberException(null,"request to %s was cancelled", m_request.getURI()));
            m_deque.offer(this);
        }

        public ListenableFuture<HttpResponse> getFuture() {
            return m_fut;
        }

        @Override
        public void susceptibleRun() throws Exception {
            m_client.get().execute(m_request, this);
        }
    }
}
