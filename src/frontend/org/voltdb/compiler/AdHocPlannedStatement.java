/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
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
