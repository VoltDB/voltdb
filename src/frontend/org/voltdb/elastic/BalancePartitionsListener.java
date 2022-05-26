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

package org.voltdb.elastic;

import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.sysprocs.BalancePartitionsRequest;

/**
 * Interface for listening to balance partitions transactions as they happen. All methods will be invoked in-line in the
 * transaction so any blocking work will hold up the transaction. These methods will only be called on the hosts which
 * are involved in the balance partitions operations so either the source or target.
 */
public interface BalancePartitionsListener {
    /**
     * Notify the listener that {@link TheHashinator} has been updated to reflect the result of this balance partitions
     * <p>
     * This will be the first method invoked during a balance partitions transaction and it will only be called once per
     * transaction.
     *
     * @param range describing the hashinator range which is being moved from one partition to another
     */
    void hashinatorUpdated(BalancePartitionsRequest.PartitionPair range);

    /**
     * Test if this listener is interested in balances involving {@code tableId}. If the listener is interested than
     * {@link #movedTuples} will be invoked.
     *
     * @param tableId  ID of the table which is going to be balanced
     * @param isSource {@code true} if this is the source of the balance partitions
     * @return {@code true} if interested
     */
    boolean isInterested(int tableId, boolean isSource);

    /**
     * Notify the listener which tuples were removed from the source partition
     *
     * @param tuples {@link VoltTable} with all of the tuples being removed
     */
    void onTuplesRemoved(VoltTable tuples);

    /**
     * Notify the listener which tuples were added from the target partition
     *
     * @param tuples {@link VoltTable} with all of the tuples being added
     */
    void onTuplesAdded(VoltTable tuples);

    /**
     * Release is called at the end of the transaction when all tuples within the selected hash range have been
     * successfully moved from the source to the target
     */
    void release();

    /**
     * Undo is called at the end of the transaction if something has failed and balance partitions is being rolled back
     */
    void undo();
}
