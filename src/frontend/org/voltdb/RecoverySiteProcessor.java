/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.RecoveryMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;

/**
 * Base class for functionality used during recovery. Derived classes implement
 * one set of functionality for the source partition and another set of functionality for the destination
 *
 */
public interface RecoverySiteProcessor {
    /**
     * doRecoveryWork loops on receiving messages. This interface is invoked
     * to handle non recovery messages.
     */
    public interface MessageHandler {
        public void handleMessage(VoltMessage message);
    }

    public void handleRecoveryMessage(RecoveryMessage message);
    public void handleSiteFaults(HashSet<Integer> failedSites, SiteTracker tracker);
    public void doRecoveryWork(long currentTxnId);
    public long bytesTransferred();
}
