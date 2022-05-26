/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.client;

/**
 * Provide the {@link Client2} with a way to throttle throughput
 * by limiting the rate of new transactions.
 *
 * Rate limiting works by slicing time into 100mS blocks, and
 * then limiting the number of transactions that can be admitted
 * within each such block. An attempt to exceed that rate is
 * handled by stalling the calling thread.
 *
 * This code is derived from {@code RateLimiter} but removes
 * outstanding-transacion-count considerations, since that is
 * now handled separately and consisently. The resulting code
 * is considerably more straightforward.
 */
class RateLimiter2 {

    final static int BLOCK_SIZE = 100; // milliseconds

    private final int targetTxnsPerBlock;
    private int currentBlockTxnCount;
    private long currentBlockTimestamp = -1;

    RateLimiter2(int txnsPerSec) {
        targetTxnsPerBlock = Math.max(1, txnsPerSec / (1000 / BLOCK_SIZE));
    }

    /**
     * We're about to send a request; this method handles rate-limiting.
     * We do the rate calculations and then either return so that the
     * request can be sent, or else we wait a tick and then retry.
     * We'll keep retrying until the rate drops below the limit.
     * There is no timeout; this is intentional since we are simulating
     * a client submitting requests at a particular maximum rate.
     */
    void limitSendRate() throws InterruptedException {
        long timestamp = System.currentTimeMillis();
        while (!rateIsWithinLimit(timestamp)) {
            Thread.sleep(1);
            timestamp = System.currentTimeMillis();
        }
    }

    /*
     * Checks current transaction rate against the target limit, to determine
     * if a further send is now possible.  If so, updates the current
     * block's transaction count (we have adnitted this request).
     */
    private synchronized boolean rateIsWithinLimit(long timestamp) {

        // Switch to a new block if 100ms has passed
        updateCurrentBlock(timestamp);

        // Check current rate (based on fraction of current block of
        // time that has actually passed)
        double fractionalLimit = targetTxnsPerBlock * (timestamp - currentBlockTimestamp + 1.0) / BLOCK_SIZE;
        if (currentBlockTxnCount < Math.ceil(fractionalLimit)) {
            ++currentBlockTxnCount;
            return true;
        }

        // Can't send yet
        return false;
    }

    /*
     * Given the timestamp of a transaction, checks whether the timestamp is
     * represented within the current block of transactions, and if not, starts
     * a new block.
     *
     * On return, we are assured that
     *   currentBlockTimestamp <= timestamp <= currentBlockTimestamp + BLOCK_SIZE
     */
    private void updateCurrentBlock(long timestamp) {
        long thisBlock = timestamp - (timestamp % BLOCK_SIZE);

        // handle first time initialization
        if (currentBlockTimestamp < 0) {
            currentBlockTimestamp = thisBlock;
        }

        // handle time moving backwards (a bit)
        if (thisBlock < currentBlockTimestamp) {
            thisBlock = currentBlockTimestamp;
        }

        // check for new block
        if (thisBlock > currentBlockTimestamp) {
            // need to deal with 100ms skips here TODO
            currentBlockTimestamp = thisBlock;
            currentBlockTxnCount = 0;
        }
    }
}
