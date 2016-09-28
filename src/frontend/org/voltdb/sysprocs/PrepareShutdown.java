/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.sysprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;

/**
 *The system stored procedure will pause the cluster and set a flag indicating that
 *the cluster is preparing for shutdown. All reads and writes except the system stored procedures which are allowed as
 *specified in SystemProcedureCatalog will be blocked.
 *
 */
@ProcInfo(singlePartition = false)
public class PrepareShutdown extends Pause
{
    @Override
    public VoltTable[] run(SystemProcedureExecutionContext ctx){
        if (ctx.isLowestSiteId()){
            VoltDB.instance().setShuttingdown(true);
        }
        return super.run(ctx);
    }
}
