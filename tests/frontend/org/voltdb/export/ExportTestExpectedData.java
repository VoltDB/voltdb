/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.exportclient.ExportDecoderBase;

public class ExportTestExpectedData {
    // hash table name + partition to verifier
    public final Map<String, ExportToSocketTestVerifier> m_verifiers = new HashMap<>();
    public final Map<String, Boolean> m_seen_verifiers = new HashMap<>();
    public final Map<String, Integer> m_expectedRowCount = new HashMap<>();

    private final Map<String, ServerListener> m_severSockets;
    // TODO: support per-table replicated stream check
    private final boolean m_replicated;
    private final boolean m_exact;
    private final long m_copies;
    public boolean m_verifySequenceNumber = true;
    public boolean m_verbose = true;

    public ExportTestExpectedData(Map<String, ServerListener> serverSockets, boolean isExportReplicated, boolean exact,
            int copies) {
        m_severSockets = serverSockets;
        m_replicated = isExportReplicated;
        m_exact = exact;
        m_copies = copies;
    }

    public synchronized void addRow(Client client, String tableName, Object partitionHash, Object[] data) {
        long partition = ((ClientImpl) client).getPartitionForParameter(VoltType.typeFromObject(partitionHash)
                .getValue(), partitionHash);
        ExportToSocketTestVerifier verifier = m_verifiers.get(tableName + partition);
        if (verifier == null) {
            verifier = new ExportToSocketTestVerifier(tableName, (int) partition);
            m_verifiers.put(tableName + partition, verifier);
            m_seen_verifiers.put(tableName + partition, Boolean.TRUE);
        }
        verifier.addRow(data);
        Integer count = m_expectedRowCount.get(tableName);
        if (count == null) {
            m_expectedRowCount.put(tableName,1);
        }
         else {
            m_expectedRowCount.put(tableName,count+1);
        }
    }

    public synchronized void verifyRows() throws Exception {
        /*
         * Process the row data in each table
         */
        for (Entry<String, ServerListener> f : m_severSockets.entrySet()) {
            String tableName = f.getKey();
            System.out.println("Processing Table:" + tableName);

            String next[] = null;
            assertEquals(getExpectedRowCount(tableName), f.getValue().getReceivedRowCount());
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
                ExportToSocketTestVerifier verifier = m_verifiers.get(tableName + partitionId);
                Long rowSeq = Long.parseLong(next[ExportDecoderBase.INTERNAL_FIELD_COUNT]);

                // verify occurrence if replicated
                if (m_replicated) {
                    assertEquals(m_copies, f.getValue().getCount(rowSeq));
                }

                assertThat( next, verifier.isExpectedRow(m_verifySequenceNumber));
            }
        }
    }

    private int getExpectedRowCount(String tableName) {
        return m_expectedRowCount.containsKey(tableName) ? m_expectedRowCount.get(tableName) : 0;
    }

    public long getExportedDataCount() {
        long retval = 0;
        for (ExportToSocketTestVerifier verifier : m_verifiers.values()) {
            retval += verifier.getSize();
        }
        return retval;
    }
}
