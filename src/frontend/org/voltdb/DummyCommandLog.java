/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.Set;
import java.util.concurrent.Semaphore;

import org.voltdb.messaging.InitiateTaskMessage;

public class DummyCommandLog implements CommandLog {
    @Override
    public void init(CatalogContext context, long txnId) {}

    @Override
    public boolean needsInitialization() {
        return false;
    }

    @Override
    public void log(InitiateTaskMessage message) {}

    @Override
    public void shutdown() throws InterruptedException {}

    @Override
    public Semaphore logFault(Set<Integer> failedSites, Set<Long> faultedTxns) {
        return new Semaphore(1);
    }

    @Override
    public void logHeartbeat(final long txnId) {}

    @Override
    public long getFaultSequenceNumber() {
        return 0;
    }

    @Override
    public void initForRejoin(CatalogContext context, long txnId,
            long faultSequenceNumber, Set<Integer> failedSites) {}
}
