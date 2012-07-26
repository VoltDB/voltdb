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

import java.util.List;

import org.voltdb.catalog.Statement;
import org.voltdb.planner.ParameterInfo;

/**
 * <p>A simple wrapper of a parameterized SQL statement. VoltDB uses this instead of
 * a Java String type for performance reasons and to cache statement meta-data like
 * result schema, compiled plan, etc..</p>
 *
 * <p>SQLStmts are used exclusively in subclasses of {@link VoltProcedure}</p>
 *
 * @see VoltProcedure
 */
public class SQLStmt {
    // Used for uncompiled SQL.
    String sqlText;
    String joinOrder;

    // Used for compiled SQL
    SQLStmtPlan plan = null;

    /**
     * Construct a SQLStmt instance from a SQL statement.
     *
     * @param sqlText Valid VoltDB compliant SQL with question marks as parameter
     * place holders.
     */
    public SQLStmt(String sqlText) {
        this(sqlText, null);
    }

    /**
     * Construct a SQLStmt instance from a SQL statement.
     *
     * @param sqlText Valid VoltDB compliant SQL with question marks as parameter
     * place holders.
     * @param joinOrder separated list of tables used by the query specifying the order they should be joined in
     */
    public SQLStmt(String sqlText, String joinOrder) {
        this.sqlText = sqlText;
        this.joinOrder = joinOrder;
    }

    /**
     * Factory method to construct a SQLStmt instance with a compiled plan attached.
     *
     * @param sqlText Valid VoltDB compliant SQL
     * @param aggregatorFragment Compiled aggregator fragment
     * @param collectorFragment Compiled collector fragment
     * @param isReplicatedTableDML Flag set to true if replicated
     * @param params Description of parameters expected by the statement
     * @return SQLStmt object with plan added
     */
    static SQLStmt createWithPlan(String sqlText,
                                  byte[] aggregatorFragment,
                                  byte[] collectorFragment,
                                  boolean isReplicatedTableDML,
                                  List<ParameterInfo> params) {
        SQLStmt stmt = new SQLStmt(sqlText, null);

        /*
         * Fill out the parameter types
         */
        if (params != null) {
            stmt.statementParamJavaTypes = new byte[params.size()];
            stmt.numStatementParamJavaTypes = params.size();
            for (ParameterInfo pi : params) {
                stmt.statementParamJavaTypes[pi.index] = pi.type.getValue();
            }
        }

        stmt.plan = new SQLStmtPlan(sqlText, aggregatorFragment, collectorFragment, isReplicatedTableDML);
        stmt.fragGUIDs = new long[collectorFragment == null ? 1 : 2];
        return stmt;
    }

    /**
     * Get the text of the SQL statement represented.
     *
     * @return String containing the text of the SQL statement represented.
     */
    public String getText() {
        return sqlText;
    }

    /**
     * Get the pre-compiled plan, if available.
     *
     * @return pre-compiled plan object or null
     */
    SQLStmtPlan getPlan() {
        return plan;
    }

    /**
     * Get the join order hint supplied in the constructor.
     *
     * @return String containing the join order hint.
     */
    public String getJoinOrder() {
        return joinOrder;
    }

    /**
     * Check if single partition.
     *
     * @return true if it is single partition
     */
    boolean isSinglePartition() {
        // Check the catalog or the plan, depending on which (if any) is available.
        return (   (this.catStmt != null && this.catStmt.getSinglepartition())
                || (this.plan != null && this.plan.getCollectorFragment() == null));
    }

    byte statementParamJavaTypes[];
    int numStatementParamJavaTypes;

    long fragGUIDs[];

    Statement catStmt = null;
}
