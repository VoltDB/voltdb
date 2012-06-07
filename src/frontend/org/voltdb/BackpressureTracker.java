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

import org.voltcore.logging.VoltLogger;

public class BackpressureTracker
{
    final private static int MAX_DESIRED_PENDING_BYTES = 67108864;
    final private static int MAX_DESIRED_PENDING_TXNS = 5000;

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final private ClientInterface m_ci;
    private int m_pendingTxnCount = 0;
    private long m_pendingTxnBytes = 0;
    private boolean m_hadBackPressure = false;

    BackpressureTracker(ClientInterface ci)
    {
        m_ci = ci;
    }

    synchronized void increaseBackpressure(int messageSize)
    {
        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (!m_hadBackPressure) {
                hostLog.info("TXN back pressure began");
                m_hadBackPressure = true;
                m_ci.onBackPressure();
            }
        }
    }

    synchronized void reduceBackpressure(int messageSize)
    {
        m_pendingTxnBytes -= messageSize;
        m_pendingTxnCount--;
        if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
            m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
        {
            if (m_hadBackPressure) {
                hostLog.info("TXN backpressure ended");
                m_hadBackPressure = false;
                m_ci.offBackPressure();
            }
        }
    }
}
