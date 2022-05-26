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
import org.voltcore.logging.VoltLogger;
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

public class Pause extends VoltSystemProcedure {
    private final static VoltLogger LOG = new VoltLogger("HOST");

    protected volatile Stat m_stat = null;
    private final static OperationMode PAUSED = OperationMode.PAUSED;

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("Pause was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    protected static  String ll(long l) {
        return Long.toString(l, Character.MAX_RADIX);
    }

    /**
     * Enter admin mode
     * @param ctx       Internal parameter. Not user-accessible.
     * @return          Standard STATUS table.
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx) {
        // Choose the lowest site ID on this host to actually flip the bit

        if (ctx.isLowestSiteId()) {
            VoltDBInterface voltdb = VoltDB.instance();
            OperationMode opMode = voltdb.getMode();
            if (LOG.isDebugEnabled()) {
                LOG.debug("voltdb opmode is " + opMode);
            }
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
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("zkMode is " + (zkMode == null ? "(null)" : OperationMode.valueOf(data)));
                        }
                        zkMode = data == null ? opMode : OperationMode.valueOf(data);
                        if (zkMode == PAUSED) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("read node at version " + stat.getVersion() + ", txn " + ll(stat.getMzxid()));
                            }
                            break;
                        }
                        stat = zk.setData(VoltZK.operationMode, PAUSED.getBytes(), stat.getVersion());
                        code = Code.OK;
                        zkMode = PAUSED;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("!WROTE! node at version " + stat.getVersion() + ", txn " + ll(stat.getMzxid()));
                        }
                        break;
                    } catch (BadVersionException ex) {
                        code = ex.code();
                    }
                } while (zkMode != PAUSED && code == Code.BADVERSION);

                m_stat = stat;
                voltdb.getHostMessenger().pause();
                voltdb.setMode(PAUSED);

             // for snmp
                SnmpTrapSender snmp = voltdb.getSnmpTrapSender();
                if (snmp != null) {
                    snmp.pause("Cluster paused.");
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Force a tick so that stats will be updated.
        // Primarily added to get latest table stats for DR pause and empty db check.
        ctx.getSiteProcedureConnection().tick();

        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
