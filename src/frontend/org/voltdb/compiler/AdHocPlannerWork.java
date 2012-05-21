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

import java.util.List;


public class AdHocPlannerWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -6567283432846270119L;

    final String sqlBatchText;
    final String[] sqlStatements;
    final Object partitionParam;

    public AdHocPlannerWork(long replySiteId, boolean shouldShutdown, long clientHandle,
            long connectionId, String hostname, boolean adminConnection, Object clientData,
            String sqlBatchText, List<String> sqlStatements, Object partitionParam)
    {
        super(replySiteId, shouldShutdown, clientHandle, connectionId, hostname,
              adminConnection, clientData);
        this.sqlBatchText = sqlBatchText;
        this.sqlStatements = sqlStatements.toArray(new String[sqlStatements.size()]);
        this.partitionParam = partitionParam;
    }

    public AdHocPlannerWork(long replySiteId, boolean shouldShutdown, long clientHandle,
            long connectionId, String hostname, boolean adminConnection,
            Object clientData, String sqlStatement, Object partitionParam)
    {
        super(replySiteId, shouldShutdown, clientHandle, connectionId, hostname,
              adminConnection, clientData);
        this.sqlBatchText = sqlStatement;
        this.sqlStatements = new String[]{sqlStatement};
        this.partitionParam = partitionParam;
    }

    public static AdHocPlannerWork forShutdown(int replySiteId) {
        return new AdHocPlannerWork(replySiteId, true, -1L, -1L, "", false, null, "", null, null);
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParam != null) ? partitionParam.toString() : "null");
        if (sqlStatements == null) {
            retval += "\n  sql: null";
        } else if (sqlStatements == null || sqlStatements.length == 0) {
            retval += "\n  sql: empty";
        } else {
            int i = 0;
            for (String sql : sqlStatements) {
                i++;
                retval += String.format("\n  sql[%d]: %s", i, sql);
            }
        }
        return retval;
    }
}
