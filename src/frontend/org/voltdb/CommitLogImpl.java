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

import java.io.*;
import java.util.*;

import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.VoltMessage;

/**
 * A commit log that manages the creation and deletion of commit quantums
 */
class CommitLogImpl implements CommitLog {

    /**
     * The current commit quantum where messages that require durability can be sent
     */
    private CommitQuantum m_currentQuantum;

    /**
     * Contains the current commit quantum as well as older ones
     * that may be deleted at some point.
     */
    private ArrayDeque<CommitQuantum> m_commitQuantums = new ArrayDeque<CommitQuantum>();

    /**
     * Whether to wait for messages to be committed to disk before delivering
     * them or deliver them immediately
     */
    private final boolean m_waitForCommit;

    /**
     * Force messages to disk every x milliseconds
     */
    private final int m_commitInterval;

    /**
     * Directory to place commit quantums in.
     */
    private final File m_commitLogDir;

    public CommitLogImpl(File commitLogDir, int commitInterval, boolean waitForCommit) throws IOException {
        m_waitForCommit = waitForCommit;
        m_commitLogDir = commitLogDir;
        m_commitInterval = commitInterval;
        m_currentQuantum = new CommitQuantum( 0, m_commitLogDir, m_commitInterval, m_waitForCommit);
        m_commitQuantums.offer(m_currentQuantum);
    }

    /* (non-Javadoc)
     * @see org.voltdb.CommitLog#logMessage(org.voltdb.messaging.VoltMessage, org.voltdb.messaging.SiteMailbox)
     */
    @Override
    public void logMessage(VoltMessage message, SiteMailbox mailbox) {
        m_currentQuantum.logMessage(message, mailbox);
    }

    /* (non-Javadoc)
     * @see org.voltdb.CommitLog#shutdown()
     */
    @Override
    public synchronized void shutdown() throws InterruptedException {
        for (CommitQuantum q : m_commitQuantums) {
            q.close();
        }
        m_commitQuantums.clear();
    }
}
