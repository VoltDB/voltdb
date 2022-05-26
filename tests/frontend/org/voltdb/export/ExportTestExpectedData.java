/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.exportclient.ExportDecoderBase;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.util.concurrent.Uninterruptibles;

public class ExportTestExpectedData {

    // Maps stream name to current information about the stream including validators for what is in each partition
    private final Map<String, StreamInformation> m_streams = new ConcurrentHashMap<>();

    private final Map<String, ServerListener> m_serverSockets;
    // TODO: support per-table replicated stream check
    private final boolean m_replicated;
    private final boolean m_exact;
    private final long m_copies;
    public boolean m_verifySequenceNumber = true;
    public boolean m_verbose = true;

    public ExportTestExpectedData(Map<String, ServerListener> serverSockets, boolean isExportReplicated, boolean exact,
            int copies) {
        m_serverSockets = serverSockets;
        m_replicated = isExportReplicated;
        m_exact = exact;
        m_copies = copies;
    }

    public void addRow(Client client, String tableName, Object partitionValue, Object[] data) {
        m_streams.computeIfAbsent(tableName, StreamInformation::new).addRow(client, partitionValue, data);
    }

    // All export tests using verifiers should ensure their clients have
    // an initialized hashinator, otherwise some calls to addRow() will be directed to
    // a partitionId of -1
    private static long getPartitionFromHashinator(Client client, Object partitionValue) {
        long partition = ((ClientImpl) client).getPartitionForParameter(
                (partitionValue == null ? VoltType.INTEGER : VoltType.typeFromObject(partitionValue)).getValue(),
                partitionValue);
        if (partition != -1) {
            return partition;
        }
        if (!client.waitForTopology(60_000)) {
            throw new RuntimeException("Timed out waiting for topology info");
        }
        partition = ((ClientImpl) client).getPartitionForParameter(
                VoltType.typeFromObject(partitionValue).getValue(), partitionValue);
        assertTrue(partition != -1);
        return partition;
    }

    public void dropStream(String stream) {
        m_streams.remove(stream);
    }

    /**
     * Wait for all rows added to this verifier or inserted by this verifier to be exported by the cluster and then
     * verify that the expected rows were received.
     * <p>
     * Note: default timeout is {@link TestExportBaseSocketExport#DEFAULT_TIMEOUT}
     *
     * @param client to use to query cluster
     * @throws Exception
     */
    public void waitForTuplesAndVerify(Client client) throws Exception {
        waitForTuplesAndVerify(client, TestExportBaseSocketExport.DEFAULT_TIMEOUT);
    }

    /**
     * Wait for all rows added to this verifier or inserted by this verifier to be exported by the cluster and then
     * verify that the expected rows were received.
     *
     * @param client  to use to query cluster
     * @param timeout to wait for all rows to be delivered
     * @throws Exception
     */
    public void waitForTuplesAndVerify(Client client, Duration timeout) throws Exception {
        waitForTuples(client, true, timeout);
        verifyRows();
    }

    /**
     * Wait for the number of tuples added to this verifier to be reflected in the stats retrieved from {@code client}
     * <p>
     * Note always waits for pending tuples to be 0
     * <p>
     * Note: default timeout is {@link TestExportBaseSocketExport#DEFAULT_TIMEOUT}
     *
     * @param client  to use to query cluster
     * @param timeout to wait for all tuples to be delivered
     * @throws Exception
     */
    public void waitForTuples(Client client) throws Exception {
        waitForTuples(client, TestExportBaseSocketExport.DEFAULT_TIMEOUT);
    }

    /**
     * Wait for the number of tuples added to this verifier to be reflected in the stats retrieved from {@code client}
     * <p>
     * Note always waits for pending tuples to be 0
     *
     * @param client  to use to query cluster
     * @param timeout to wait for all tuples to be delivered
     * @throws Exception
     */
    public void waitForTuples(Client client, Duration timeout) throws Exception {
        waitForTuples(client, true, timeout);
    }

    /**
     * Wait for the number of tuples added to this verifier to be reflected in the stats retrieved from {@code client}
     *
     * @param client         to use to query cluster
     * @param waitForPending If {@code true} this will wait for pending tuples to be 0
     * @param timeout        to wait for all tuples to be delivered
     * @throws Exception
     */
    public void waitForTuples(Client client, boolean waitForPending, Duration timeout) throws Exception {
        TestExportBaseSocketExport.waitForExportRowsByPartitionToBeDelivered(client,
                Maps.transformValues(m_streams,
                        i -> Maps.transformValues(i.m_verifiers, ExportToSocketTestVerifier::getTotalCount)),
                waitForPending, timeout);
    }

    public void verifyRows() throws Exception {
        /*
         * Process the row data in each table
         */
        for (Entry<String, ServerListener> f : m_serverSockets.entrySet()) {
            String tableName = f.getKey();
            System.out.println("Processing Table:" + tableName);

            String next[] = null;
            if (!m_exact) {
                continue;
            }
            while ((next = f.getValue().getNext()) != null) {
                final int partitionId = Integer.valueOf(next[3]);
                if (m_verbose) {
                    StringBuilder sb = new StringBuilder();
                    for (String s : next) {
                        sb.append(s).append(", ");
                    }
                    System.out.println(sb);
                }
                ExportToSocketTestVerifier verifier = getVerifier(tableName, partitionId);
                Long rowSeq = Long.parseLong(next[ExportDecoderBase.INTERNAL_FIELD_COUNT]);

                // verify occurrence if replicated
                if (m_replicated) {
                    assertEquals(m_copies, f.getValue().getCount(rowSeq));
                }
                if (verifier != null) {
                    assertThat( next, verifier.isExpectedRow(m_verifySequenceNumber));
                }
            }
        }
    }

    public synchronized void ignoreRow(String tableName, long rowId) {
        ServerListener listener = m_serverSockets.get(tableName);
        if (listener != null) {
            listener.ignoreRow(rowId);
        }
    }

    private ExportToSocketTestVerifier getVerifier(String stream, int partition) {
        StreamInformation info = m_streams.get(stream);
        return info == null ? null : info.getVerifier(partition);
    }

    public long getExportedDataCount() {
        return m_streams.values().stream().mapToLong(StreamInformation::getTupleCount).sum();
    }

    private static final class StreamInformation {
        private long m_tupleCount;
        final String m_name;
        final Map<Integer, ExportToSocketTestVerifier> m_verifiers = new HashMap<>();

        StreamInformation(String name) {
            m_name = name;
        }

        synchronized void addRow(Client client, Object partitionValue, Object[] row) {
            int partition = (int) getPartitionFromHashinator(client, partitionValue);
            m_verifiers.computeIfAbsent(partition, p -> new ExportToSocketTestVerifier(m_name, partition)).addRow(row);
            ++m_tupleCount;
        }

        synchronized long getTupleCount() {
            return m_tupleCount;
        }

        synchronized ExportToSocketTestVerifier getVerifier(Integer partition) {
            return m_verifiers.get(partition);
        }
    }
}
