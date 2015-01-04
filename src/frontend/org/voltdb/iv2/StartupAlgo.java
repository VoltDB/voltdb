/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import com.google_voltpatches.common.util.concurrent.SettableFuture;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.Pair;

public class StartupAlgo implements RepairAlgo
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final CountDownLatch m_missingStartupSites;
    private final int m_partitionId;

    // Each Term can process at most one promotion; if promotion fails, make
    // a new Term and try again (if that's your big plan...)
    private final SettableFuture<Long> m_promotionResult = SettableFuture.create();

    /**
     * Setup a new StartupAlgo but don't take any action to take responsibility.
     */
    public StartupAlgo(CountDownLatch missingStartupSites, String whoami, int partitionId)
    {
        if (missingStartupSites != null) {
            m_missingStartupSites = missingStartupSites;
        }
        else {
            m_missingStartupSites = new CountDownLatch(0);
        }
        m_partitionId = partitionId;
        m_whoami = whoami;
    }

    @Override
    public Future<Long> start()
    {
        try {
            prepareForStartup();
        } catch (Exception e) {
            tmLog.error(m_whoami + "failed leader promotion:", e);
            m_promotionResult.setException(e);
        }
        return m_promotionResult;
    }

    @Override
    public boolean cancel()
    {
        // Can't cancel startup this way, Ned.
        return false;
    }

    /** Block until all replicas are present. */
    void prepareForStartup()
        throws InterruptedException
    {
        tmLog.info(m_whoami +
                "starting leader promotion.  Waiting for " +
                m_missingStartupSites.getCount() + " more for configured k-safety.");

        // block here until the babysitter thread provides all replicas.
        // then initialize the mailbox's replica set and proceed as leader.
        m_missingStartupSites.await();
        m_promotionResult.set(TxnEgo.makeZero(m_partitionId).getTxnId());
    }

    /** Process a new repair log response */
    @Override
    public void deliver(VoltMessage message)
    {
        throw new RuntimeException("Dude, shouldn't get these here.");
    }
}
