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

package org.voltdb.iv2;

/**
 * An interest that is registered at each partition replica
 * and is called when transaction commits on all replicas.
 *
 * Implement this interface and register it to the RepairLog
 * if your code cares about when transaction commits globally.
 */
public interface TransactionCommitInterest {
    /**
     * Called when transactions are committed on all replicas.
     *
     * This callback will be invoked from the site thread.
     * DO NOT perform any blocking operation in the callback.
     *
     * @param spHandle    The highest safe spHandle across all replicas.
     *                    Use this to determine if a transaction is safe
     *                    to be made visible to other parts of the system.
     */
    void transactionCommitted(final long spHandle);
}
