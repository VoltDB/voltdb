/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.rejoin;

import java.io.IOException;

import org.voltcore.messaging.TransactionInfoBaseMessage;

/**
 * A task message queue.
 */
public interface TaskLog {
    /**
     * Log a task message.
     *
     * @param message
     */
    public void logTask(TransactionInfoBaseMessage message) throws IOException;

    /**
     * Pop the next task message from the head of the queue.
     *
     * @return null if there is no more messages or the message is being
     *         retrieved asynchronously.
     */
    public TransactionInfoBaseMessage getNextMessage() throws IOException;

    /**
     * If the queue is empty
     * @return
     */
    public boolean isEmpty() throws IOException;

    /**
     * Close the task log
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * Default policy at startup is to drop invocations until recording is necessary
     * When used for live rejoin the first SnapshotSave plan fragment triggers the start
     * of recording of transactions for the live rejoin.
     *
     * @param snapshotSpHandle    Note that it is possible that this may be called
     *                            multiple times with different snapshotSpHandles during
     *                            live rejoin. There may be multiple snapshot fragments
     *                            with the same snapshot nonce due to snapshot collisions.
     *                            At the time this is called, we don't know if the
     *                            snapshot will succeed or not. If it collides with an
     *                            snapshot in progress, it will be retried later. The task
     *                            log implementation should update the snapshotSpHandle
     *                            with the latest one.
     */
    public void enableRecording(long snapshotSpHandle);
}
