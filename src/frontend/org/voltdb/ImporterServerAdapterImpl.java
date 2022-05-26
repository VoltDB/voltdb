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

package org.voltdb;

import java.util.function.Predicate;

import org.voltdb.client.ProcedureCallback;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.ImporterServerAdapter;
import org.voltdb.importer.ImporterStatsCollector;

/**
 * Implementation that uses the server internal classes to execute procedures and
 * to report information for statistics collection.
 */
public class ImporterServerAdapterImpl implements ImporterServerAdapter {
    private ImporterStatsCollector m_statsCollector;

    public ImporterServerAdapterImpl(ImporterStatsCollector statsCollector) {
        m_statsCollector = statsCollector;
    }

    /**
     * Returns true if a table with the given name exists in the server catalog.
     */
    public boolean hasTable(String name) {
        return getInternalConnectionHandler().hasTable(name);
    }

    @Override
    public boolean callProcedure(AbstractImporter importer, Predicate<Integer> backPressurePredicate,
            ProcedureCallback procCallback, String proc, Object... fieldList) {
        return getInternalConnectionHandler()
                .callProcedure(importer, backPressurePredicate, m_statsCollector, procCallback, proc, fieldList);
    }

    private InternalConnectionHandler getInternalConnectionHandler() {
        return VoltDB.instance().getClientInterface().getInternalConnectionHandler();
    }

    @Override
    public void reportFailure(String importerName, String procName, boolean decrementPending) {
        m_statsCollector.reportFailure(importerName, procName, decrementPending);
    }

    @Override
    public void reportQueued(String importerName, String procName) {
        m_statsCollector.reportQueued(importerName, procName);
    }

    @Override
    public void reportInitialized(String importerName, String procName) {
        m_statsCollector.reportInitialized(importerName, procName);
    }
}
