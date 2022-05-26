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

import org.voltdb.common.Constants;


/**
 * Provides access to some non-public SQLStmt methods needed for ad hoc queries.
 */
public class SQLStmtAdHocHelper {
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
    public static SQLStmt createWithPlan(byte[] sqlText,
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
        return SQLStmt.createWithPlan(sqlText,
                aggFragId, aggPlanHash, isAggTransactional,
                collectorFragId, collectorPlanHash, isCollectorTransactional,
                isReplicatedTableDML, isReadOnly, params, site);
    }

    public static byte[] getSQLBytes(SQLStmt sqlStmt) {
        if (sqlStmt.sqlText == null) {
            sqlStmt.sqlText = sqlStmt.sqlTextStr.getBytes(Constants.UTF8ENCODING);
        }
        return sqlStmt.sqlText;
    }

    public static void setSQLBytes(SQLStmt sqlStmt, byte[] sql) {
        sqlStmt.sqlText = sql;
        sqlStmt.sqlTextStr = null;
    }

    public static void setSQLStr(SQLStmt sqlStmt, String sql) {
        sqlStmt.sqlText = null;
        sqlStmt.sqlTextStr = sql;
    }

    public static int getHash(SQLStmt sqlStmt) {
        return sqlStmt.sqlCRC;
    }

    /**
     * <p>Queue the adhoc SQL statement for execution. The adhoc SQL statement will have
     * to be planned which is orders of magnitude slower then using a precompiled SQL statements.</p>
     *
     * <p>If the query is parameterized it is possible to pass in the parameters.</p>
     *
     * <p>This method is hidden here so users won't be distracted by it, and so people will be less
     * likely to try it. It's not removed outright because it's a feature we DO intend to ship at some
     * point, and there's some value in making sure it doesn't regress. It's currently used in a
     * few tests and in txnid-selfcheck2.</p>
     *
     * @deprecated This method is experimental and not intended for production use yet.
     * @param sql An ad-hoc SQL string to be run transactionally in this procedure.
     * @param args Parameter values for the SQL string.
     */
    @Deprecated
    public static void voltQueueSQLExperimental(VoltProcedure proc, String sql, Object... args) {
        proc.m_runner.voltQueueSQL(sql, args);
    }
}
