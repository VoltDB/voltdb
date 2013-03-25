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

    /**
     * Per-fragment info
     */
    static class Frag {
        long id;
        byte[] planHash;
        boolean transactional;
    }

    // Used for uncompiled SQL.
    byte[] sqlText;
    String joinOrder;
    // hash of the sql string for determinism checks
    byte[] sqlCRC;

    byte statementParamJavaTypes[];

    Frag aggregator;
    Frag collector;

    boolean isReplicatedTableDML;
    boolean isReadOnly;

    boolean inCatalog;

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

        inCatalog = true;
    }

    /**
     * Factory method to construct a SQLStmt instance from a plan outside the catalog.
     *
     * @param sqlText Valid VoltDB compliant SQL
     * @param aggFragId Site-local id of the aggregator fragment
     * @param aggPlanHash 20 byte sha1 hash of the aggregator fragment plan
     * @param isAggTransactional Does the aggregator fragment read/write tables?
     * @param collectorFragId Site-local id of the collector fragment
     * @param collectorPlanHash 20 byte sha1 hash of the collector fragment plan
     * @param isCollectorTransactional Does the collector fragment read/write tables?
     * @param isReplicatedTableDML Flag set to true if replicated DML
     * @param isReadOnly Is SQL read only?
     * @param params Description of parameters expected by the statement
     * @return SQLStmt object with plan added
     */
    static SQLStmt createWithPlan(byte[] sqlText,
                                  long aggFragId,
                                  byte[] aggPlanHash,
                                  boolean isAggTransactional,
                                  long collectorFragId,
                                  byte[] collectorPlanHash,
                                  boolean isCollectorTransactional,
                                  boolean isReplicatedTableDML,
                                  boolean isReadOnly,
                                  VoltType[] params) {
        SQLStmt stmt = new SQLStmt(sqlText, null);

        stmt.aggregator = new SQLStmt.Frag();
        stmt.aggregator.id = aggFragId;
        stmt.aggregator.planHash = aggPlanHash;
        stmt.aggregator.transactional = isAggTransactional;

        if (collectorFragId > 0) {
            stmt.collector = new SQLStmt.Frag();
            stmt.collector.id = collectorFragId;
            stmt.collector.planHash = collectorPlanHash;
            stmt.collector.transactional = isCollectorTransactional;
        }

        /*
         * Fill out the parameter types
         */
        if (params != null) {
            stmt.statementParamJavaTypes = new byte[params.length];
            for (int i = 0; i < params.length; i++) {
                stmt.statementParamJavaTypes[i] = params[i].getValue();
            }
        }

        stmt.isReadOnly = isReadOnly;
        stmt.isReplicatedTableDML = isReplicatedTableDML;
        stmt.inCatalog = false;

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
     * Get the join order hint supplied in the constructor.
     *
     * @return String containing the join order hint.
     */
    public String getJoinOrder() {
        return joinOrder;
    }
}
