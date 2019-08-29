/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

public class AckingContainer extends BBContainer {
    ExportDataSource m_source;
    final long m_startSeqNo;
    final long m_lastSeqNo;
    final BBContainer m_backingCont;
    // Note: the schema doesn't need an explicit discard
    long m_startTime = 0;
    private boolean m_discarded;
    private static VoltLogger EXPORT_LOG = new VoltLogger("EXPORT");

    private AckingContainer(ExportDataSource source, BBContainer cont,
                            long startSeqNo, long lastSeqNo) {
        super(cont.b());
        m_source = source;
        m_startSeqNo = startSeqNo;
        m_lastSeqNo = lastSeqNo;
        m_backingCont = cont;
        m_discarded = false;
    }

    public static AckingContainer create(ExportDataSource source,
                                     StreamBlock sb,
                                     StreamBlockQueue sbq) {
        // We always want to have a schema
        return new AckingContainer(source,
                        sb.unreleasedContainer(),
                        sb.startSequenceNumber(),
                        sb.startSequenceNumber() + sb.rowCount() - 1);
    }

    public void updateStartTime(long startTime) {
        m_startTime = startTime;
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
        m_discarded = true;
    }

    // Package private
    void internalDiscard() {
        internalDiscard(true);
    }

    public boolean isDiscarded() {
        return m_discarded;
    }

    @Override
    // Invoked from GuestProcessor after the container is delivered.
    public void discard() {
        checkDoubleFree();
        try {
            m_source.advance(m_lastSeqNo, m_startTime);
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
        return new String("Container: ending at " + m_lastSeqNo);
    }
}