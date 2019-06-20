/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
package org.voltdb.export;

import java.util.concurrent.RejectedExecutionException;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.exportclient.ExportRowSchema;

public class AckingContainer extends BBContainer {
    ExportDataSource m_source;
    final long m_startSeqNo;
    final long m_lastSeqNo;
    final long m_commitSeqNo;
    final BBContainer m_backingCont;
    // Note: the schema doesn't need an explicit discard
    ExportRowSchema m_schema;
    long m_startTime = 0;
    long m_commitSpHandle = 0;
    private static VoltLogger EXPORT_LOG = new VoltLogger("EXPORT");

    private AckingContainer(ExportDataSource source, BBContainer cont,
                            ExportRowSchema schema, long startSeqNo, long lastSeqNo, long commitSeq) {
        super(cont.b());
        m_source = source;
        m_startSeqNo = startSeqNo;
        m_lastSeqNo = lastSeqNo;
        m_commitSeqNo = commitSeq;
        m_backingCont = cont;
        m_schema = schema;
    }

    public static AckingContainer create(ExportDataSource source,
                                     StreamBlock sb,
                                     StreamBlockQueue sbq) {
        // We always want to have a schema
        assert(sb.getSchema() != null);
        return new AckingContainer(source,
                        sb.unreleasedContainer(),
                        sb.getSchema(),
                        sb.startSequenceNumber(),
                        sb.startSequenceNumber() + sb.rowCount() - 1,
                        sb.committedSequenceNumber());
    }

    public void updateStartTime(long startTime) {
        m_startTime = startTime;
    }

    public ExportRowSchema getSchema() {
        return m_schema;
    }

    public long getCommittedSeqNo() {
        return m_commitSeqNo;
    }

    public void setCommittedSpHandle(long spHandle) {
        m_commitSpHandle = spHandle;
    }

    long getStartSeqNo() {
        return m_startSeqNo;
    }

    // Package private
    long getLastSeqNo() {
        return m_lastSeqNo;
    }

    private void internalDiscard(boolean checkDoubleFree) {
        if (checkDoubleFree) {
            checkDoubleFree();
        }
        m_backingCont.discard();
    }

    // Package private
    void internalDiscard() {
        internalDiscard(true);
    }

    @Override
    // Invoked from GuestProcessor after the container is delivered.
    public void discard() {
        checkDoubleFree();
        try {
            m_source.advance(m_lastSeqNo, m_commitSeqNo, m_commitSpHandle, m_startTime);
        } catch (RejectedExecutionException rej) {
            //Don't expect this to happen outside of test, but in test it's harmless
            if (EXPORT_LOG.isDebugEnabled()) {
                EXPORT_LOG.debug("Acking export data task rejected, this should be harmless");
            }
        } finally {
            internalDiscard(false);
        }
    }

    @Override
    public String toString() {
        return new String("Container: ending at " + m_lastSeqNo + " (Committed " + m_commitSeqNo + ")");
    }
}
