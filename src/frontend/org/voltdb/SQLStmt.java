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

package org.voltdb;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.voltdb.common.Constants;
import org.voltdb.planner.ActivePlanRepository;

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
        final long id;
        final byte[] planHash;
        final boolean transactional;

        Frag(long id, byte[] planHash, boolean transactional) {
            this.id = id;
            this.planHash = planHash;
            this.transactional = transactional;
        }
    }

    // Used for non-compiled SQL.
    byte[] sqlText;
    String sqlTextStr;
    String joinOrder;
    // hash of the SQL string for determinism checks
    int sqlCRC;

    byte statementParamTypes[];

    Frag aggregator;
    Frag collector;

    boolean isReplicatedTableDML;
    boolean isReadOnly;

    boolean inCatalog;

    String stmtName = null;
    protected void setStmtName(String name) {
        stmtName = name;
    }
    protected String getStmtName() {
        return stmtName;
    }

    // used to clean up plans
    SiteProcedureConnection site;

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
        this(canonicalizeStmt(sqlText).getBytes(Constants.UTF8ENCODING), joinOrder);
    }

    /**
     * Construct a SQLStmt instance from a byte array for internal use.
     */
    protected SQLStmt(byte[] sqlText, String joinOrder) {
        this.sqlText = sqlText;
        this.joinOrder = joinOrder;

        // create a hash for determinism purposes
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(sqlText);
        // this will sometimes go negative in the cast, but should be 1-1
        this.sqlCRC = (int) crc.getValue();

        inCatalog = true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        // I have some doubts as to whether site is ever null, whether it ever should be,
        // and whether it should be the determining factor in this cleanup code.
        // "site" is only used here and arguably abused as a null/non-null boolean.
        // The interesting cases to consider are:
        // - the standard SQLStmts that are defined in user VoltProcedure classes.
        // Oddly, these can be static members or instance members based on the whim of the user
        //  -- are SQLStmts in either of these cases "finalized" before site shutdown,
        // at a time when refreshing them in the LRU isn't pointless?
        // - the dynamic SQLStmts built for ad hoc queries.
        // These include client-issued ad hocs and "experimental" stored-proc-issued ad hocs.
        // I'm totally guessing that site==null was supposed to distinguish somehow among
        // some of these cases but I don't really know, or know which or know why or know how. --paul
        if (site != null) {
            ActivePlanRepository.decrefPlanFragmentById(aggregator.id);
            if (collector != null) {
                ActivePlanRepository.decrefPlanFragmentById(collector.id);
            }
        }

        super.finalize();
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
     * @param site SPC used for cleanup of plans
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
                                  VoltType[] params,
                                  SiteProcedureConnection site) {
        SQLStmt stmt = new SQLStmt(sqlText, null);

        stmt.aggregator = new SQLStmt.Frag(aggFragId, aggPlanHash, isAggTransactional);

        if (collectorFragId > 0) {
            stmt.collector = new SQLStmt.Frag(collectorFragId, collectorPlanHash, isCollectorTransactional);
        }

        /*
         * Fill out the parameter types
         */
        if (params != null) {
            stmt.statementParamTypes = new byte[params.length];
            for (int i = 0; i < params.length; i++) {
                stmt.statementParamTypes[i] = params[i].getValue();
            }
        }

        stmt.isReadOnly = isReadOnly;
        stmt.isReplicatedTableDML = isReplicatedTableDML;
        stmt.inCatalog = false;

        stmt.site = site;

        return stmt;
    }

    /**
     * Get the text of the SQL statement represented.
     *
     * @return String containing the text of the SQL statement represented.
     */
    public String getText() {
        if (sqlTextStr == null) {
            sqlTextStr = new String(sqlText, Constants.UTF8ENCODING);
        }
        return sqlTextStr;
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
     * Is this a read only statement?
     *
     * @return true if it's read only, false otherwise
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    // In SQL statement the input without ending with a semicolon is legitimate,
    // however in order to do a reverse look up (crc -> sql str), we'd like to
    // use the same statement to compute crc.
    public static String canonicalizeStmt(String stmtStr) {
        // Cleanup whitespace newlines and adding semicolon for catalog compatibility
        stmtStr = stmtStr.replaceAll("\n", " ");
        stmtStr = stmtStr.trim();

        if (!stmtStr.endsWith(";")) {
            stmtStr += ";";
        }
        return stmtStr;
    }

    public void setInCatalog(boolean inCatalog) {
        this.inCatalog = inCatalog;
    }


}
