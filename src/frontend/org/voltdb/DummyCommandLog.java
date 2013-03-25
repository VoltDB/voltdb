/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import java.util.Map;
import java.util.Set;

import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class DummyCommandLog implements CommandLog {
    @Override
    public void init(CatalogContext context, long txnId, Map<Integer, Long> perPartitionTxnId, String affinity) {}

    @Override
    public boolean needsInitialization() {
        return false;
    }

    @Override
    public void shutdown() throws InterruptedException {}

    @Override
    public void initForRejoin(CatalogContext context, long txnId, Map<Integer, Long> perPartitionTxnId, boolean isRejoin, String affinity) {}

    @Override
    public boolean log(
            Iv2InitiateTaskMessage message,
            long spHandle,
            DurabilityListener l,
            Object handle) {
        return false;
    }

    @Override
    public void logIv2Fault(long writerHSId, Set<Long> survivorHSId,
            int partitionId, long spHandle) {
    }

    @Override
    public boolean isEnabled()
    {
        // No real command log, obviously not enabled
        return false;
    }
}
