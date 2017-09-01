/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importclient.kafka10;

import java.util.concurrent.atomic.AtomicBoolean;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.kafka.util.PendingWorkTracker;
import org.voltdb.importer.CommitTracker;

public class InvocationProcedureCallback implements ProcedureCallback {

    private final long m_nextoffset;
    private final long m_offset;
    private final PendingWorkTracker m_callbackTracker;
    private final CommitTracker m_tracker;
    private final AtomicBoolean m_dontCommit;

    public InvocationProcedureCallback(
            final long curoffset,
            final long nextoffset,
            final PendingWorkTracker callbackTracker,
            final CommitTracker tracker,
            final AtomicBoolean dontCommit) {
        m_offset = curoffset;
        m_nextoffset = nextoffset;
        m_callbackTracker = callbackTracker;
        m_tracker = tracker;
        m_dontCommit = dontCommit;
    }

    @Override
    public void clientCallback(ClientResponse response) throws Exception {
        m_callbackTracker.consumeWork();
        if (!m_dontCommit.get() && response.getStatus() != ClientResponse.SERVER_UNAVAILABLE) {
            m_tracker.commit(m_nextoffset);
        }
    }

    public long getOffset() {
        return m_offset;
    }
}
