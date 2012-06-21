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

import org.voltdb.CatalogContext;


public class AdHocPlannerWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -6567283432846270119L;

    final String sqlBatchText;
    final String[] sqlStatements;
    final Object partitionParam;
    final CatalogContext catalogContext;
    final boolean allowParameterization;

    public AdHocPlannerWork(long replySiteId, boolean shouldShutdown, long clientHandle,
            long connectionId, String hostname, boolean adminConnection, Object clientData,
            String sqlBatchText, List<String> sqlStatements, Object partitionParam, CatalogContext context,
            boolean allowParameterization)
    {
        super(replySiteId, shouldShutdown, clientHandle, connectionId, hostname,
              adminConnection, clientData);
        this.sqlBatchText = sqlBatchText;
        this.sqlStatements = sqlStatements.toArray(new String[sqlStatements.size()]);
        this.partitionParam = partitionParam;
        this.catalogContext = context;
        this.allowParameterization = allowParameterization;
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParam != null) ? partitionParam.toString() : "null");
        assert(sqlStatements != null);
        if (sqlStatements.length == 0) {
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
