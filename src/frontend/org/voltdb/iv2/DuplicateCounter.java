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

package org.voltdb.iv2;

import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;

import org.voltdb.utils.MiscUtils;

/**
 * Track responses from each partition. This should be subsumed
 * by proper response tracking for the usual replication case?
 */
public class DuplicateCounter
{
    static final int MISMATCH = 0;
    static final int DONE = 1;
    static final int WAITING = 2;

    final AtomicInteger m_expectedResponses;
    final long m_destinationId;
    Long m_responseHash = null;

    protected DuplicateCounter(
            long destinationHSId,
            int expectedResponses,
            long realTxnId)
    {
        m_expectedResponses = new AtomicInteger(expectedResponses);
        m_destinationId = destinationHSId;
    }

    private int checkCommon(long hash)
    {
        if (m_responseHash == null) {
            m_responseHash = Long.valueOf(hash);
        }
        else if (!m_responseHash.equals(hash)) {
            System.out.printf("COMPARING: %d to %d\n", hash, m_responseHash);
            return MISMATCH;
        }

        if (m_expectedResponses.decrementAndGet() == 0) {
            return DONE;
        }
        else {
            return WAITING;
        }
    }

    int offer(InitiateResponseMessage message)
    {
        long hash = message.getClientResponseData().getHashOfTableResults();
        return checkCommon(hash);
    }

    int offer(FragmentResponseMessage message)
    {
        long hash = 0;
        for (int i = 0; i < message.getTableCount(); i++) {
            hash ^= MiscUtils.cheesyBufferCheckSum(message.getTableAtIndex(i).getBuffer());
        }
        return checkCommon(hash);
    }
}
