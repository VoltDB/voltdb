/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

/**
 * Execute the supplied XML string using org.apache.log4j.xml.DomConfigurator
 * The first parameter is the string containing the XML configuration
 */
public class UpdateLogging extends VoltSystemProcedure
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private final static CyclicBarrier barrier = new CyclicBarrier(VoltDB.instance().getCatalogContext().getNodeSettings().getLocalSitesCount());

    private static final VoltLogger loggers[] = new VoltLogger[] {
            new VoltLogger("HOST")
    };

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public boolean allowableSysprocForTaskLog() {
        return true;
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context)
    {
        throw new RuntimeException("UpdateLogging was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    /**
     * Change the operational log configuration.
     * @param ctx       Internal parameter. Not user-accessible.
     * @param xmlConfig New configuration XML document.
     * @return          Standard STATUS table.
     */
    @SuppressWarnings("deprecation")
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
                           String username,
                           String remoteHost,
                           String xmlConfig)
    {
        long oldLevels  = 0;
        if (ctx.isLowestSiteId()) {
            // Logger level is a global property, pick the site with lowest id to do it.
            hostLog.info(String.format("%s from %s changed the log4j settings", username, remoteHost));
            hostLog.info(xmlConfig);
            oldLevels = hostLog.getLogLevels(loggers);
        }

        try {
            // Mimic the multi-fragment semantics as scatter-gather pattern is an overkill for this simple task.
            // There are chances that some sites being interrupted and update the logging before old logger level
            // being read, but the reasons we don't care because 1) it is rare and 2) it only effects when HOST
            // logger being changed from higher than INFO level to INFO or lower level.
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException dontcare) { }

        VoltDB.instance().logUpdate(xmlConfig, DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this),
                ctx.getPaths().getVoltDBRoot());
        ctx.updateBackendLogLevels();

        if (ctx.isLowestSiteId()) {
            long newLevels = hostLog.getLogLevels(loggers);
            if (newLevels != oldLevels) {
                // If HOST logger wasn't able to log before and now it can, logs the setting change event.
                int index = (int)((oldLevels >> 3) & 7);
                Level before = Level.values()[index];
                index = (int)((newLevels >> 3) & 7);
                Level after = Level.values()[index];
                if (before.ordinal() > Level.INFO.ordinal() && after.ordinal() <= Level.INFO.ordinal()) {
                    hostLog.info(String.format("%s from %s changed the log4j settings", username, remoteHost));
                    hostLog.info(xmlConfig);
                }
            }
            barrier.reset();
        }

        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
