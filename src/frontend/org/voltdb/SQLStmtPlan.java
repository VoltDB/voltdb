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

    private final String sqlText;
    private final String aggregatorFragment;
    private final String collectorFragment;
    private final boolean isReplicatedTableDML;

    /**
     * Constructor
     *
     * @param sqlText               VoltDB SQL text
     * @param aggregatorFragment    Compiled plan aggregator fragment
     * @param collectorFragment     Compiled plan collector fragment
     * @param isReplicatedTableDML  True if replicated
     */
    public SQLStmtPlan(String sqlText, String aggregatorFragment, String collectorFragment,
            boolean isReplicatedTableDML) {
        this.sqlText = sqlText;
        this.aggregatorFragment = aggregatorFragment;
        this.collectorFragment = collectorFragment;
        this.isReplicatedTableDML = isReplicatedTableDML;
    }

    /**
     * Get the SQL text
     * @return  SQL text
     */
    public String getSqlText() {
        return sqlText;
    }

    /**
     * Get the plan aggregator fragment
     * @return  the plan aggregator fragment string
     */
    public String getAggregatorFragment() {
        return aggregatorFragment;
    }

    /**
     * Get the plan collector fragment
     * @return  the plan collector fragment string
     */
    public String getCollectorFragment() {
        return collectorFragment;
    }

    /**
     * Get the replication flag
     * @return  true if replicated
     */
    public boolean isReplicatedTableDML() {
        return isReplicatedTableDML;
    }
}