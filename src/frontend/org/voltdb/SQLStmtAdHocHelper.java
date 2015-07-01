/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
}
