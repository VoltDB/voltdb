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

public class AdHocPlannerWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -6567283432846270119L;

    final String sql;
    final Object partitionParam;

    public AdHocPlannerWork(boolean shouldShutdown, long clientHandle,
            long connectionId, String hostname, boolean adminConnection,
            Object clientData, String sql, Object partitionParam)
    {
        super(shouldShutdown, clientHandle, connectionId, hostname,
                adminConnection, clientData);
        this.sql = sql;
        this.partitionParam = partitionParam;
    }

    public static AdHocPlannerWork forShutdown() {
        return new AdHocPlannerWork(true, -1L, -1L, "", false, null, "", null);
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParam != null) ? partitionParam.toString() : "null");
        retval += "\n  sql: " + ((sql != null) ? sql : "null");
        return retval;
    }
}
