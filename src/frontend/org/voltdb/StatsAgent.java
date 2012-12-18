/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.utils.CompressionService;

/**
 * Agent responsible for collecting stats on this host.
 *
 */
public class StatsAgent {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final byte JSON_PAYLOAD = 0;
    private static final byte STATS_PAYLOAD = 1;
    private static final int MAX_IN_FLIGHT_REQUESTS = 5;
    static int STATS_COLLECTION_TIMEOUT = 60 * 1000;

    // The id that responds to global requests (catalog id == -1).
    // Start at -1 until a winner is chosen.
    // Updated in synchronized getStats() method.
    private static Long m_idForGlobalStats = null;

    private long m_nextRequestId = 0;
    private Mailbox m_mailbox;
    private final ScheduledThreadPoolExecutor m_es =
        org.voltcore.utils.CoreUtils.getScheduledThreadPoolExecutor("StatsAgent", 1, CoreUtils.SMALL_STACK_SIZE);

    private final HashMap<SysProcSelector, HashMap<Long, ArrayList<StatsSource>>> registeredStatsSources =
        new HashMap<SysProcSelector, HashMap<Long, ArrayList<StatsSource>>>();

    private final HashSet<SysProcSelector> handledSelectors = new HashSet<SysProcSelector>();

    private static class PendingStatsRequest {
        private final String selector;
        private final Connection c;
        private final long clientData;
        private int expectedStatsResponses = 0;
        private final VoltTable aggregateTables[];
        private final long startTime;
        public PendingStatsRequest(
                String selector,
                Connection c,
                long clientData,
                VoltTable aggregateTables[],
                long startTime) {
            this.startTime = startTime;
            this.selector = selector;
            this.c = c;
            this.clientData = clientData;
            this.aggregateTables = aggregateTables;
        }
    }

    private final Map<Long, PendingStatsRequest> m_pendingRequests = new HashMap<Long, PendingStatsRequest>();

    public StatsAgent() {
        SysProcSelector selectors[] = SysProcSelector.values();
        for (int ii = 0; ii < selectors.length; ii++) {
            registeredStatsSources.put(selectors[ii], new HashMap<Long, ArrayList<StatsSource>>());
        }
        handledSelectors.add(SysProcSelector.PROCEDURE);
        handledSelectors.add(SysProcSelector.PLANNER);
    }

    public void getMailbox(final HostMessenger hostMessenger, final long hsId) {
        m_mailbox = new LocalMailbox(hostMessenger, hsId) {
            @Override
            public void deliver(final VoltMessage message) {
                m_es.submit(new Runnable() {
                    @Override
                    public void run() {
                        handleMailboxMessage(message);
                    }
                });
            }
        };
        hostMessenger.registerMailbox(m_mailbox);
    }

    private void handleMailboxMessage(VoltMessage message) {
        try {
            if (message instanceof LocalObjectMessage) {
                LocalObjectMessage lom = (LocalObjectMessage)message;
                ((Runnable)lom.payload).run();
            } else if (message instanceof BinaryPayloadMessage) {
                BinaryPayloadMessage bpm = (BinaryPayloadMessage)message;
                byte payload[] = CompressionService.decompressBytes(bpm.m_payload);
                if (bpm.m_metadata[0] == JSON_PAYLOAD) {
                    String jsonString = new String(payload, "UTF-8");
                    JSONObject obj = new JSONObject(jsonString);
                    handleJSONMessage(obj);
                } else if (bpm.m_metadata[0] == STATS_PAYLOAD) {
                    handleStatsResponse(payload);
                }
            }
        } catch (Throwable e) {
            hostLog.error("Exception processing message in stats agent " + message, e);
        }

    }

    private void handleStatsResponse(byte[] payload) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        Long requestId = buf.getLong();

        PendingStatsRequest request = m_pendingRequests.get(requestId);
        if (request == null) {
            hostLog.warn("Received a stats response for stats request " + requestId + " that no longer exists");
            return;
        }

        if (buf.hasRemaining()) {
            for (int ii = 0; ii < request.aggregateTables.length; ii++) {
                final int tableLength = buf.getInt();
                int oldLimit = buf.limit();
                buf.limit(buf.position() + tableLength);
                ByteBuffer tableBuf = buf.slice();
                buf.position(buf.limit()).limit(oldLimit);
                //Lazy init the aggregate table using the first result
                if (request.aggregateTables[ii] == null) {
                    ByteBuffer copy = ByteBuffer.allocate(tableBuf.capacity() * 2);
                    copy.put(tableBuf);
                    copy.limit(copy.position());
                    copy.position(0);
                    VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer( copy, false);
                    request.aggregateTables[ii] = vt;
                } else {
                    VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer( tableBuf, true);
                    while (vt.advanceRow()) {
                        request.aggregateTables[ii].add(vt);
                    }
                }
            }
        }

        request.expectedStatsResponses--;
        if (request.expectedStatsResponses > 0) return;

        m_pendingRequests.remove(requestId);
        sendStatsResponse(request);
    }

    public void collectStats(final Connection c, final long clientHandle, final String selector) throws Exception {
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    collectStatsImpl(c, clientHandle, selector);
                } catch (Throwable e) {
                    hostLog.warn("Exception while attempting to collect stats", e);
                }
            }
        });
    }

    private void collectStatsImpl(Connection c, long clientHandle, String selector) throws Exception {
        if (m_pendingRequests.size() > MAX_IN_FLIGHT_REQUESTS) {
            /*
             * Defensively check for an expired request not caught
             * by timeout check. Should never happen.
             */
            Iterator<PendingStatsRequest> iter = m_pendingRequests.values().iterator();
            final long now = System.currentTimeMillis();
            boolean foundExpiredRequest = false;
            while (iter.hasNext()) {
                PendingStatsRequest psr = iter.next();
                if (now - psr.startTime > STATS_COLLECTION_TIMEOUT * 2) {
                    iter.remove();
                    foundExpiredRequest = true;
                }
            }
            if (!foundExpiredRequest) {
                final ClientResponseImpl errorResponse =
                    new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                         new VoltTable[0], "Too many pending stat requests", clientHandle);
                ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                errorResponse.flattenToBuffer(buf).flip();
                c.writeStream().enqueue(buf);
                return;
            }
        }

        if (selector.equals("TOPO")) {
            if (!VoltDB.instance().isIV2Enabled()) {
                final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                             new VoltTable[0], "IV2 is not enabled", clientHandle);
                ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                errorResponse.flattenToBuffer(buf).flip();
                c.writeStream().enqueue(buf);
                return;
            }
            PendingStatsRequest psr = new PendingStatsRequest(
                selector,
                c,
                clientHandle,
                new VoltTable[1],
                System.currentTimeMillis());
            collectTopoStats(psr);
            return;
        }

        PendingStatsRequest psr =
            new PendingStatsRequest(
                    selector,
                    c,
                    clientHandle,
                    new VoltTable[2],
                    System.currentTimeMillis());
        final long requestId = m_nextRequestId++;
        m_pendingRequests.put(requestId, psr);
        m_es.schedule(new Runnable() {
            @Override
            public void run() {
                checkForRequestTimeout(requestId);
            }
        },
        STATS_COLLECTION_TIMEOUT,
        TimeUnit.MILLISECONDS);

        JSONObject obj = new JSONObject();
        obj.put("requestId", requestId);
        obj.put("returnAddress", m_mailbox.getHSId());
        obj.put("selector", "DRNODE");
        byte payloadBytes[] = CompressionService.compressBytes(obj.toString(4).getBytes("UTF-8"));
        final SiteTracker st = VoltDB.instance().getSiteTracker();
        for (long agent : st.getStatsAgents()) {
            psr.expectedStatsResponses++;
            BinaryPayloadMessage bpm = new BinaryPayloadMessage(new byte[] {JSON_PAYLOAD}, payloadBytes);
            m_mailbox.send(agent, bpm);
        }
    }

    private void checkForRequestTimeout(long requestId) {
        PendingStatsRequest psr = m_pendingRequests.remove(requestId);
        if (psr == null) {
            return;
        }
        hostLog.warn("Stats request " + requestId + " timed out, sending error to client");

        ClientResponseImpl response =
            new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                    null,
                    new VoltTable[0], "Stats request hit sixty second timeout before all responses were received");
        response.setClientHandle(psr.clientData);
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf).flip();
        psr.c.writeStream().enqueue(buf);
    }

    private void sendStatsResponse(PendingStatsRequest request) throws Exception {
        byte statusCode = ClientResponse.SUCCESS;
        String statusString = null;
        /*
         * It is possible not to receive a table response if a feature is not enabled
         */
        VoltTable responseTables[] = request.aggregateTables;
        for (int ii = 0; ii < responseTables.length; ii++) {
            if (responseTables[ii] == null) {
                responseTables = new VoltTable[0];
                statusCode = ClientResponse.GRACEFUL_FAILURE;
                statusString =
                    "Requested statistic \"" + request.selector +
                    "\" is not supported in the current configuration";
                break;
            }
        }

        ClientResponseImpl response =
            new ClientResponseImpl(statusCode, ClientResponse.UNINITIALIZED_APP_STATUS_CODE, null, responseTables, statusString);
        response.setClientHandle(request.clientData);
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf).flip();
        request.c.writeStream().enqueue(buf);
    }

    private void handleJSONMessage(JSONObject obj) throws Exception {
        String selectorString = obj.getString("selector");
        SysProcSelector selector = SysProcSelector.valueOf(selectorString);
        if (selector == SysProcSelector.DRNODE) {
            collectDRStats(obj);
        }
    }

    private void collectTopoStats(PendingStatsRequest psr)
    {
        List<Long> catalogIds = Arrays.asList(new Long[] { 0L });
        psr.aggregateTables[0] = getStats(SysProcSelector.TOPO, catalogIds, false, psr.startTime);
        try {
            sendStatsResponse(psr);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Screwed", true, e);
        }
    }

    private void collectDRStats(JSONObject obj) throws Exception {
        List<Long> catalogIds = Arrays.asList(new Long[] { 0L });
        Long now = System.currentTimeMillis();
        long requestId = obj.getLong("requestId");
        long returnAddress = obj.getLong("returnAddress");

        VoltTable partitionStats = getStats(SysProcSelector.DRPARTITION, catalogIds, false, now);
        VoltTable nodeStats = getStats(SysProcSelector.DRNODE, catalogIds, false, now);

        /*
         * Send a response with no data since the stats is not supported
         */
        if (partitionStats == null || nodeStats == null) {
            ByteBuffer responseBuffer = ByteBuffer.allocate(8);
            responseBuffer.putLong(requestId);
            byte responseBytes[] = CompressionService.compressBytes(responseBuffer.array());
            BinaryPayloadMessage bpm = new BinaryPayloadMessage( new byte[] {STATS_PAYLOAD}, responseBytes);
            m_mailbox.send(returnAddress, bpm);
            return;
        }

        ByteBuffer partitionStatsBuffer = partitionStats.getBuffer();
        partitionStatsBuffer.position(0);

        ByteBuffer nodeStatsBuffer = nodeStats.getBuffer();
        nodeStatsBuffer.position(0);

        ByteBuffer responseBuffer = ByteBuffer.allocate(
                16 //requestId + a length prefix for each stats table
                + partitionStatsBuffer.remaining() + nodeStatsBuffer.remaining());
        responseBuffer.putLong(requestId);
        responseBuffer.putInt(partitionStatsBuffer.remaining());
        responseBuffer.put(partitionStatsBuffer);
        responseBuffer.putInt(nodeStatsBuffer.remaining());
        responseBuffer.put(nodeStatsBuffer);
        byte responseBytes[] = CompressionService.compressBytes(responseBuffer.array());

        BinaryPayloadMessage bpm = new BinaryPayloadMessage( new byte[] {STATS_PAYLOAD}, responseBytes);
        m_mailbox.send(returnAddress, bpm);
    }

    public synchronized void registerStatsSource(SysProcSelector selector, long catalogId, StatsSource source) {
        assert selector != null;
        assert source != null;
        final HashMap<Long, ArrayList<StatsSource>> catalogIdToStatsSources = registeredStatsSources.get(selector);
        assert catalogIdToStatsSources != null;
        ArrayList<StatsSource> statsSources = catalogIdToStatsSources.get(catalogId);
        if (statsSources == null) {
            statsSources = new ArrayList<StatsSource>();
            catalogIdToStatsSources.put(catalogId, statsSources);
        }
        statsSources.add(source);
    }

    /**
     * Get statistics.
     * @param selector    @Statistics selector keyword
     * @param catalogIds  statistics catalog ids
     * @param interval    true if processing a reporting interval
     * @param now         current timestamp
     * @return  statistics VoltTable results
     */
    public synchronized VoltTable getStats(
            final SysProcSelector selector,
            final List<Long> catalogIds,
            final boolean interval,
            final Long now) {
        return getStatsInternal(selector, catalogIds, interval, now, null);
    }

    /**
     * Get statistics once for global stats, e.g. PLANNER. Chooses an arbitrary site
     * as the one that triggers retrievals.
     * @param selector  statistics selector keyword
     * @param interval  true if processing a reporting interval
     * @param now       current timestamp
     * @param siteId    siteId for catalog
     * @return  statistics VoltTable results
     */
    public synchronized VoltTable getSiteAndHostStats(
            final SysProcSelector selector,
            final boolean interval,
            final Long now,
            final long siteId) {
        // If it's the first site make it the chosen site for host-global statistics.
        if (m_idForGlobalStats == null) {
            m_idForGlobalStats = siteId;
        }

        // First get the site-specific statistics.
        VoltTable results;
        {
            ArrayList<Long> catalogIds = new ArrayList<Long>();
            catalogIds.add(siteId);
            results = getStatsInternal(selector, catalogIds, interval, now, null);
        }

        // Then append global results if it's the chosen site.
        if (siteId == m_idForGlobalStats) {
            // -1 is always the global catalog id.
            ArrayList<Long> catalogIds = new ArrayList<Long>();
            catalogIds.add(-1L);
            results = getStatsInternal(selector, catalogIds, interval, now, results);
        }

        return results;
    }

    /**
     * Internal statistics retrieval. Optionally append results to an existing
     * result set. This is used by getSiteAndHostStats() for PLANNER statistics.
     * @param selector     statistics selector keyword
     * @param interval     true if processing a reporting interval
     * @param now          current timestamp
     * @param prevResults  previous results, if any, to append to
     * @return  statistics VoltTable results
     */
    private synchronized VoltTable getStatsInternal(
            final SysProcSelector selector,
            final List<Long> catalogIds,
            final boolean interval,
            final Long now,
            VoltTable prevResults) {
        assert selector != null;
        assert catalogIds != null;
        assert catalogIds.size() > 0;
        final HashMap<Long, ArrayList<StatsSource>> catalogIdToStatsSources = registeredStatsSources.get(selector);
        assert catalogIdToStatsSources != null;

        ArrayList<StatsSource> statsSources = catalogIdToStatsSources.get(catalogIds.get(0));
        //Let these two be null since they are for pro features
        if (selector == SysProcSelector.DRNODE || selector == SysProcSelector.DRPARTITION) {
            if (statsSources == null || statsSources.isEmpty()) {
                return null;
            }
        } else {
            assert statsSources != null && !statsSources.isEmpty();
        }

        /*
         * Some sources like TableStats use VoltTable to keep track of
         * statistics. We need to use the table schema the VoltTable has in this
         * case.
         */
        VoltTable.ColumnInfo columns[] = null;
        if (!statsSources.get(0).isEEStats())
            columns = statsSources.get(0).getColumnSchema().toArray(new VoltTable.ColumnInfo[0]);
        else {
            final VoltTable table = statsSources.get(0).getStatsTable();
            if (table == null)
                return null;
            columns = new VoltTable.ColumnInfo[table.getColumnCount()];
            for (int i = 0; i < columns.length; i++)
                columns[i] = new VoltTable.ColumnInfo(table.getColumnName(i),
                                                      table.getColumnType(i));
        }

         // Append to previous results if provided.
        final VoltTable resultTable = prevResults != null ? prevResults : new VoltTable(columns);

        for (Long catalogId : catalogIds) {
            statsSources = catalogIdToStatsSources.get(catalogId);
            assert statsSources != null;
            for (final StatsSource ss : statsSources) {
                assert ss != null;

                /*
                 * Some sources like TableStats use VoltTable to keep track of
                 * statistics
                 */
                if (ss.isEEStats()) {
                    final VoltTable table = ss.getStatsTable();
                    // this table can be null during recovery, at least
                    if (table != null) {
                        while (table.advanceRow()) {
                            resultTable.add(table);
                        }
                        table.resetRowPosition();
                    }
                } else {
                    Object statsRows[][] = ss.getStatsRows(interval, now);
                    for (Object[] row : statsRows) {
                        resultTable.addRow(row);
                    }
                }
            }
        }
        return resultTable;
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_es.awaitTermination(1, TimeUnit.DAYS);
    }
}
