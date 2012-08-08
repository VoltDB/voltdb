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

import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionInitiator;

/**
 * The default command log reinitiator for community edition VoltDB.
 */
public class DefaultCommandLogReinitiator implements CommandLogReinitiator
{
    private Callback m_callback;

    @Override
    public void setCallback(Callback callback) {
        m_callback = callback;
    }

    @Override
    public void replay() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (m_callback != null) {
                    m_callback.onReplayCompletion();
                }
            }
        }).start();
    }

    @Override
    public void join() throws InterruptedException {
    }

    @Override
    public boolean hasReplayedSegments() {
        return false;
    }

    @Override
    public Long getMaxLastSeenTxn() {
        return null;
    }

    @Override
    public boolean started() {
        return true;
    }

    @Override
    public void setSnapshotTxnId(long txnId) {
    }

    @Override
    public void returnAllSegments() {
    }

    @Override
    public boolean hasReplayedTxns() {
        return false;
    }

    @Override
    public void generateReplayPlan() {
    }

    @Override
    public void setCatalogContext(CatalogContext context) {
    }

    @Override
    public void setInitiator(TransactionInitiator initiator) {
    }

    @Override
    public void setSiteTracker(SiteTracker siteTracker) {
    }
};
