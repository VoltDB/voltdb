/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import java.nio.ByteBuffer;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.voltdb.catalog.Statement;

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
    byte[] sqlText;
    String joinOrder;
    // hash of the sql string for determinism checks
    byte[] sqlCRC;

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
        this(sqlText.getBytes(VoltDB.UTF8ENCODING), joinOrder);
    }

    /**
     * Construct a SQLStmt instance from a byte array for internal use.
     */
    private SQLStmt(byte[] sqlText, String joinOrder) {
        this.sqlText = sqlText;
        this.joinOrder = joinOrder;

        // create a hash for determinism purposes
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(sqlText);
        // ugly hack to get bytes from an int
        this.sqlCRC = ByteBuffer.allocate(4).putInt((int) crc.getValue()).array();
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
    static SQLStmt createWithPlan(byte[] sqlText,
                                  byte[] aggregatorFragment,
                                  byte[] collectorFragment,
                                  boolean isReplicatedTableDML,
                                  boolean isReadOnly,
                                  VoltType[] params) {
        SQLStmt stmt = new SQLStmt(sqlText, null);

        /*
         * Fill out the parameter types
         */
        if (params != null) {
            stmt.statementParamJavaTypes = new byte[params.length];
            stmt.numStatementParamJavaTypes = params.length;
            for (int i = 0; i < params.length; i++) {
                stmt.statementParamJavaTypes[i] = params[i].getValue();
            }
        }

        stmt.plan = new SQLStmtPlan(aggregatorFragment, collectorFragment, isReplicatedTableDML, isReadOnly);
        return stmt;
    }

    /**
     * Get the text of the SQL statement represented.
     *
     * @return String containing the text of the SQL statement represented.
     */
    public String getText() {
        return new String(sqlText, VoltDB.UTF8ENCODING);
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

    boolean isReadOnly() {
        // Check the catalog or the plan, depending on which (if any) is available.
        return (   (this.catStmt != null && this.catStmt.getReadonly())
                || (this.plan != null && this.plan.isReadOnly()));
    }

    byte statementParamJavaTypes[];
    int numStatementParamJavaTypes;

    long fragGUIDs[];
    int numFragGUIDs;

    Statement catStmt = null;
}
