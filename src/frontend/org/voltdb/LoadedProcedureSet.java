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

package org.voltdb;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.logging.Level;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.utils.LogKeys;

public class LoadedProcedureSet {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    // user procedures.
    final HashMap<String, ProcedureRunner> procs =
        new HashMap<String, ProcedureRunner>(16, (float) .1);

    // map of sysproc fragment ids to system procedures.
    final HashMap<Long, ProcedureRunner> m_registeredSysProcPlanFragments =
        new HashMap<Long, ProcedureRunner>();

    final ProcedureRunnerFactory m_runnerFactory;
    final long m_siteId;
    final int m_siteIndex;
    final int m_numberOfPartitions;
    final SiteProcedureConnection m_site;

    public LoadedProcedureSet(ExecutionSite site, ProcedureRunnerFactory runnerFactory, long siteId, int siteIndex, int numberOfPartitions) {
        m_runnerFactory = runnerFactory;
        m_siteId = siteId;
        m_siteIndex = siteIndex;
        m_numberOfPartitions = numberOfPartitions;
        m_site = site;
    }

    public ProcedureRunner getSysproc(long fragmentId) {
        synchronized (m_registeredSysProcPlanFragments) {
            return m_registeredSysProcPlanFragments.get(fragmentId);
        }
    }

    public void registerPlanFragment(final long pfId, final ProcedureRunner proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false);
            m_registeredSysProcPlanFragments.put(pfId, proc);
        }
    }

    void loadProcedures(
            CatalogContext catalogContext,
            BackendTarget backendTarget,
            CatalogSpecificPlanner csp) {
        procs.clear();
        m_registeredSysProcPlanFragments.clear();
        loadProceduresFromCatalog(catalogContext, backendTarget, csp);
        loadSystemProcedures(catalogContext, backendTarget, csp);
    }

    private void loadProceduresFromCatalog(
            CatalogContext catalogContext,
            BackendTarget backendTarget,
            CatalogSpecificPlanner csp) {
        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = catalogContext.database.getProcedures();
        for (final Procedure proc : catalogProcedures) {

            // Sysprocs used to be in the catalog. Now they aren't. Ignore
            // sysprocs found in old catalog versions. (PRO-365)
            if (proc.getTypeName().startsWith("@")) {
                continue;
            }

            ProcedureRunner runner = null;
            VoltProcedure procedure = null;
            if (proc.getHasjava()) {
                final String className = proc.getClassname();
                Class<?> procClass = null;
                try {
                    procClass = catalogContext.classForProcedure(className);
                }
                catch (final ClassNotFoundException e) {
                    if (className.startsWith("org.voltdb.")) {
                        VoltDB.crashLocalVoltDB("VoltDB does not support procedures with package names " +
                                                        "that are prefixed with \"org.voltdb\". Please use a different " +
                                                        "package name and retry.", false, null);
                    }
                    else {
                        VoltDB.crashLocalVoltDB("VoltDB was unable to load a procedure it expected to be in the " +
                                                "catalog jarfile and will now exit.", false, null);
                    }
                }
                try {
                    procedure = (VoltProcedure) procClass.newInstance();
                }
                catch (final InstantiationException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { m_siteId, m_siteIndex }, e);
                }
                catch (final IllegalAccessException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { m_siteId, m_siteIndex }, e);
                }
            }
            else {
                procedure = new ProcedureRunner.StmtProcedure();
            }

            assert(procedure != null);
            runner = m_runnerFactory.create(procedure, proc, csp);
            procs.put(proc.getTypeName(), runner);
        }
    }

    private void loadSystemProcedures(
            CatalogContext catalogContext,
            BackendTarget backendTarget,
            CatalogSpecificPlanner csp) {
        Set<Entry<String,Config>> entrySet = SystemProcedureCatalog.listing.entrySet();
        for (Entry<String, Config> entry : entrySet) {
            Config sysProc = entry.getValue();
            Procedure proc = sysProc.asCatalogProcedure();

            VoltSystemProcedure procedure = null;
            ProcedureRunner runner = null;

            final String className = sysProc.getClassname();
            Class<?> procClass = null;
            try {
                procClass = catalogContext.classForProcedure(className);
            }
            catch (final ClassNotFoundException e) {
                if (sysProc.commercial) {
                    continue;
                }
                hostLog.l7dlog(
                        Level.WARN,
                        LogKeys.host_ExecutionSite_GenericException.name(),
                        new Object[] { m_siteId, m_siteIndex },
                        e);
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            try {
                procedure = (VoltSystemProcedure) procClass.newInstance();
            }
            catch (final InstantiationException e) {
                hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                        new Object[] { m_siteId, m_siteIndex }, e);
            }
            catch (final IllegalAccessException e) {
                hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                        new Object[] { m_siteId, m_siteIndex }, e);
            }

            runner = m_runnerFactory.create(procedure, proc, csp);
            procedure.initSysProc(m_numberOfPartitions, m_site, this, proc, catalogContext.cluster);
            procs.put(entry.getKey(), runner);
        }
    }

}
