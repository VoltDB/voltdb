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

public class SQLStmtPlan {

    private final byte[] aggregatorFragment;
    private final byte[] collectorFragment;
    private final boolean isReplicatedTableDML;
    private final boolean isReadOnly;

    /**
     * Constructor
     *
     * @param aggregatorFragment    Compiled plan aggregator fragment
     * @param collectorFragment     Compiled plan collector fragment
     * @param isReplicatedTableDML  True if replicated
     * @param isReadOnly            True if read only
     */
    public SQLStmtPlan(byte[] aggregatorFragment, byte[] collectorFragment,
            boolean isReplicatedTableDML, boolean isReadOnly) {
        this.aggregatorFragment = aggregatorFragment;
        this.collectorFragment = collectorFragment;
        this.isReplicatedTableDML = isReplicatedTableDML;
        this.isReadOnly = isReadOnly;
    }

    /**
     * Get the plan aggregator fragment
     * @return  the plan aggregator fragment string
     */
    public byte[] getAggregatorFragment() {
        return aggregatorFragment;
    }

    /**
     * Get the plan collector fragment
     * @return  the plan collector fragment string
     */
    public byte[] getCollectorFragment() {
        return collectorFragment;
    }

    /**
     * Get the replication flag
     * @return  true if replicated
     */
    public boolean isReplicatedTableDML() {
        return isReplicatedTableDML;
    }

    /**
     * Get the read only flag
     * @return  true if read only
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }
}