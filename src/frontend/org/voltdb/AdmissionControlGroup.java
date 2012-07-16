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


import java.util.HashSet;
import org.voltcore.logging.VoltLogger;

public class AdmissionControlGroup implements org.voltcore.network.QueueMonitor
{
    final private int MAX_DESIRED_PENDING_BYTES;
    final private int MAX_DESIRED_PENDING_TXNS;

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private int m_pendingTxnCount = 0;
    private long m_pendingTxnBytes = 0;
    private boolean m_hadBackPressure = false;
    private final long m_expectedThreadId = Thread.currentThread().getId();
    public interface ACGMember
    {
        public void onBackpressure();
        public void offBackpressure();
    }

    private final HashSet<ACGMember> m_members = new HashSet<ACGMember>();

    public AdmissionControlGroup(int maxBytes, int maxRequests)
    {
        MAX_DESIRED_PENDING_BYTES = maxBytes;
        MAX_DESIRED_PENDING_TXNS = maxRequests;
    }

    public void addMember(ACGMember member)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        m_members.add(member);
    }

    public void removeMember(ACGMember member)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        m_members.remove(member);
    }

    public void increaseBackpressure(int messageSize)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (!m_hadBackPressure) {
                hostLog.debug("TXN back pressure began");
                System.out.println("TXN back pressure began");
                m_hadBackPressure = true;
                for (ACGMember m : m_members) {
                    m.onBackpressure();
                }
            }
        }
    }

    public void reduceBackpressure(int messageSize)
    {
        assert(m_expectedThreadId == Thread.currentThread().getId());
        m_pendingTxnBytes -= messageSize;
        m_pendingTxnCount--;
        if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
            m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
        {
            if (m_hadBackPressure) {
                hostLog.debug("TXN backpressure ended");
                System.out.println("TXN back pressure ended");
                m_hadBackPressure = false;
                for (ACGMember m : m_members) {
                    m.offBackpressure();
                }
            }
        }
    }

    public boolean hasBackPressure() {
        return m_hadBackPressure;
    }

    @Override
    public boolean queue(int bytes) {
        if (bytes > 0) {
            m_pendingTxnBytes += bytes;
            if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES) {
                if (!m_hadBackPressure) {
                    hostLog.debug("TXN back pressure began");
                    System.out.println("TXN back pressure began");
                    m_hadBackPressure = true;
                    for (ACGMember m : m_members) {
                        m.onBackpressure();
                    }
                }
            }
        } else {
            m_pendingTxnBytes += bytes;
            if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8)) {
                if (m_hadBackPressure) {
                    hostLog.debug("TXN backpressure ended");
                    System.out.println("TXN back pressure ended");
                    m_hadBackPressure = false;
                    for (ACGMember m : m_members) {
                        m.offBackpressure();
                    }
                }
            }
        }

        return false;
    }
}
