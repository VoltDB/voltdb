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

package org.voltdb.export;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.RealVoltDB;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;

/**
 * @author rdykiel
 *
 * Generic Export Manager Interface, also exposes singleton ExportManager instance.
 */
public interface ExportManagerInterface {

    static AtomicReference<ExportManagerInterface> m_self = new AtomicReference<>();

    public static ExportManagerInterface instance() {
        return m_self.get();
}

    public static void setInstanceForTest(ExportManagerInterface self) {
        m_self.set(self);
    }


    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     */
    // FIXME - this synchronizes on the ExportManager class, but everyone else synchronizes on the instance.
    public static void initialize(
            Constructor<?> constructor,
            int myHostId,
            CatalogContext catalogContext,
            boolean isRejoin,
            boolean forceCreate,
            HostMessenger messenger,
            List<Pair<Integer, Integer>> partitions)
            throws ExportManagerInterface.SetupException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
        ExportManagerInterface em = (ExportManagerInterface) constructor.newInstance(myHostId, catalogContext, messenger);
        m_self.set(em);
        if (forceCreate) {
            em.clearOverflowData();
        }
        em.initialize(catalogContext, partitions, isRejoin);

        RealVoltDB db=(RealVoltDB)VoltDB.instance();
        db.getStatsAgent().registerStatsSource(StatsSelector.EXPORT,
                myHostId, // m_siteId,
                em.getExportStats());
    }

    /**
     * Thrown if the initial setup of the loader fails
     */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;

        SetupException(final String msg) {
            super(msg);
        }

        SetupException(final Throwable cause) {
            super(cause);
        }
    }

    public void clearOverflowData() throws ExportManagerInterface.SetupException;

    public int getConnCount();

    public Generation getGeneration();

    public ExportStats getExportStats();

    public int getExportTablesCount();

    public List<ExportStatsRow> getStats(final boolean interval);

    public void initialize(CatalogContext catalogContext, List<Pair<Integer, Integer>> localPartitionsToSites,
            boolean isRejoin);

    public void prepareAcceptMastership(int partitionId);

    public void prepareTransferMastership(int partitionId, int hostId);

    public void takeMastership(int partitionId);

    public void shutdown();

    public void startPolling(CatalogContext catalogContext);

    public void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE,
            boolean requiresNewExportGeneration, List<Pair<Integer, Integer>> localPartitionsToSites);

    public void updateInitialExportStateToSeqNo(int partitionId, String signature,
            boolean isRecover, long sequenceNumber);
}
