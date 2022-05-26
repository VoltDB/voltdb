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

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.exportclient.ExportDecoderBase;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import org.voltdb.exportclient.ExportRow;

public class ExportTestVerifier extends ExportDecoderBase
{
    private final ArrayDeque<ExportRow> m_data;
    private final ArrayDeque<Object[]> m_expected_data;
    private boolean m_rowFailed = false;
    private final int m_partitionId;
    private long sequenceNumber = 0;
    private static final VoltLogger m_logger = new VoltLogger("CONSOLE");
    public static boolean m_closed = false;
    public static boolean m_paused = false;
    private final ListeningExecutorService m_es;
    public static boolean m_verifySequenceNumber = false;

    ExportTestVerifier(AdvertisedDataSource source)
    {
        super(source);
        m_partitionId = source.partitionId;
        m_data = new ArrayDeque<ExportRow>();
        m_expected_data = new ArrayDeque<Object[]>();
        m_es = CoreUtils.getListeningSingleThreadExecutor(
                "Test Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
        System.out.println("VERIFIER for: " + source.partitionId);
    }

    @Override
    public ListeningExecutorService getExecutor() {
        return m_es;
    }

    synchronized void addRow(Object[] data) {
        m_expected_data.add(data);
    }

    long getSize() {
        return m_data.size();
    }

    @Override
    public void onBlockStart(ExportRow r) throws RestartBlockException {
        //long flag = 0;
        if (m_closed) {
            return;
        }
        if (m_paused) {
            throw new RestartBlockException(true);
        } else {
            return;
        }
    }

    @Override
    public boolean processRow(ExportRow rd) throws RestartBlockException {
        if (m_paused) {
            throw new RestartBlockException(true);
        }
        Object[] decoded = rd.values;

        //System.out.println("Process Row Called found data.");
        m_data.offer(rd);
        //m_logger.info("Adding Data: " + m_data.size());
        if (ExportTestVerifier.m_verifySequenceNumber) {
            if (!decoded[2].equals(sequenceNumber)) {
                System.out.println("Failed on sequence number expected:" + sequenceNumber + " exported:" + decoded[2]);
                m_rowFailed = true;
            }
            sequenceNumber++;
        }
        return true;
    }

    boolean done()
    {
        return empty();
    }

    boolean empty()
    {
        return (m_data.isEmpty());
    }

    boolean allRowsVerified() {
        System.out.println("Verifying Row Consumed: " + m_data.size()
                + " Expected: " + m_expected_data.size() + " And Rows Failed: " + m_rowFailed);
        boolean result = (m_expected_data.size() == m_data.size() && (!m_rowFailed));
        if (!result) {
            System.out.println("ExportVerifier error. partition ID: " + m_partitionId);
            System.out.println("  Data size: " +
                               m_data.size() + " row failed state: " + m_rowFailed);
        } else {
            System.out.println("Export verification successful.");
        }
        return result;

    }

    @Override
    public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        m_es.shutdown();
        try {
            m_es.awaitTermination(356, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
        System.out.println("Source No Longer Present: " + m_partitionId);
    }
}
