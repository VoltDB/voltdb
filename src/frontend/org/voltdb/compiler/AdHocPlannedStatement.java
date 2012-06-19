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

package org.voltdb.compiler;

/**
 * Holds the plan and original SQL source for a single statement.
 *
 * Will typically be contained by AdHocPlannedStmtBatch. Both this class and the batch extend
 * AsyncCompilerResult to allow working at either the batch or the individual statement level.
 */
public class AdHocPlannedStatement extends AsyncCompilerResult implements Cloneable {
    private static final long serialVersionUID = 1144100816601598092L;
    public String sql;
    public String aggregatorFragment;
    public String collectorFragment;
    public boolean isReplicatedTableDML;
    public Object partitionParam;
    public int catalogVersion;

    /***
     * Constructor
     *
     * @param sql                       SQL statement source
     * @param aggregatorFragment        planned aggregator fragment
     * @param collectorFragment         planned collector fragment
     * @param isReplicatedTableDML      replication flag
     * @param partitionParam partition  parameter
     * @param catalogVersion            catalog version
     */
    public AdHocPlannedStatement(String sql,
                                 String aggregatorFragment,
                                 String collectorFragment,
                                 boolean isReplicatedTableDML,
                                 Object partitionParam,
                                 int catalogVersion) {
        this.sql = sql;
        this.aggregatorFragment = aggregatorFragment;
        this.collectorFragment = collectorFragment;
        this.isReplicatedTableDML = isReplicatedTableDML;
        this.partitionParam = partitionParam;
        this.catalogVersion = catalogVersion;
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParam != null) ? partitionParam.toString() : "null");
        retval += "\n  sql: " + ((sql != null) ? sql : "null");
        return retval;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
