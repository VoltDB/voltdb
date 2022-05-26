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

import org.apache.zookeeper_voltpatches.KeeperException.BadVersionException;
import org.apache.zookeeper_voltpatches.KeeperException.Code;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltdb.DependencyPair;
import org.voltdb.OperationMode;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;
import org.voltdb.snmp.SnmpTrapSender;

public class Resume extends VoltSystemProcedure {

    protected volatile Stat m_stat = null;
    private final static OperationMode RUNNING = OperationMode.RUNNING;
    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("Resume was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    /**
     * Exit admin mode
     * @param ctx       Internal parameter. Not user-accessible.
     * @return          Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        // Choose the lowest site ID on this host to actually flip the bit
        VoltDBInterface voltdb = VoltDB.instance();
        OperationMode opMode = voltdb.getMode();

        if (ctx.isLowestSiteId()) {
            ZooKeeper zk = voltdb.getHostMessenger().getZK();
            try {
                Stat stat;
                OperationMode zkMode = null;
                Code code;
                do {
                    stat = new Stat();
                    code = Code.BADVERSION;
                    try {
                        byte [] data = zk.getData(VoltZK.operationMode, false, stat);
                        zkMode = data == null ? opMode : OperationMode.valueOf(data);
                        if (zkMode == RUNNING) {
                            break;
                        }
                        stat = zk.setData(VoltZK.operationMode, RUNNING.getBytes(), stat.getVersion());
                        code = Code.OK;
                        zkMode = RUNNING;
                        break;
                    } catch (BadVersionException ex) {
                        code = ex.code();
                    }
                } while (zkMode != RUNNING && code == Code.BADVERSION);

                m_stat = stat;
                voltdb.getHostMessenger().unpause();
                voltdb.setMode(RUNNING);
                // for snmp
                SnmpTrapSender snmp = voltdb.getSnmpTrapSender();
                if (snmp != null) {
                    snmp.resume("Cluster resumed.");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return new VoltTable[] {t};
    }
}
