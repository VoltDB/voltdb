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

package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.RealVoltDB;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

// The system procedure is used to move partition leader from one host to
// another. It will be executed on the partition master
public class MigratePartitionLeader extends VoltSystemProcedure {
    private final static VoltLogger log = new VoltLogger("HOST");

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, int partitionKey,
            int partitionId, int hostId) throws VoltAbortException
    {
        String info = String.format("[@MigratePartitionLeader] Moving the leader for partition %d to host %d.",
                partitionId, hostId);
        log.info(info);

        ColumnInfo[] column = new ColumnInfo[2];
        column[0] = new ColumnInfo("STATUS", VoltType.BIGINT);
        column[1] = new ColumnInfo("MESSAGE", VoltType.STRING);
        VoltTable t = new VoltTable(column);

        if (partitionId != ctx.getPartitionId()) {
            String msg = String.format("[@MigratePartitionLeader] Executed at a wrong partition %d for partition %d.",
                    ctx.getPartitionId(), partitionId);
            t.addRow(VoltSystemProcedure.STATUS_FAILURE, msg);
            return (new VoltTable[] {t});
        }

        RealVoltDB db = (RealVoltDB)VoltDB.instance();
        Long targetHsid = db.getCartographer().getHSIDForPartitionHost(hostId, partitionId);
        if (targetHsid == null) {
            String msg = String.format("[@MigratePartitionLeader] The host %d is invalid.", hostId);
            t.addRow(VoltSystemProcedure.STATUS_FAILURE, msg);
            return (new VoltTable[] {t});
        }

        t.addRow(VoltSystemProcedure.STATUS_OK, "");
        return (new VoltTable[] {t});
    }

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }
}
