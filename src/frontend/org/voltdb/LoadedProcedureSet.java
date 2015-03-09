/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.compiler.Language;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.groovy.GroovyScriptProcedureDelegate;
import org.voltdb.utils.LogKeys;

import com.google_voltpatches.common.collect.ImmutableMap;

public class LoadedProcedureSet {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // user procedures.
    ImmutableMap<String, ProcedureRunner> procs = ImmutableMap.<String, ProcedureRunner>builder().build();
    // cached default procs
    Map<String, ProcedureRunner> m_defaultProcCache = new HashMap<>();

    // map of sysproc fragment ids to system procedures.
    final HashMap<Long, ProcedureRunner> m_registeredSysProcPlanFragments =
        new HashMap<Long, ProcedureRunner>();

    final ProcedureRunnerFactory m_runnerFactory;
    CatalogSpecificPlanner m_csp = null;
    PlannerTool m_plannerTool = null;
    DefaultProcedureManager m_defaultProcManager = null;
    final long m_siteId;
    final int m_siteIndex;
    final SiteProcedureConnection m_site;

    public LoadedProcedureSet(SiteProcedureConnection site, ProcedureRunnerFactory runnerFactory, long siteId, int siteIndex) {
        m_runnerFactory = runnerFactory;
        m_siteId = siteId;
        m_siteIndex = siteIndex;
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

    public void loadProcedures(
            CatalogContext catalogContext,
            BackendTarget backendTarget,
            CatalogSpecificPlanner csp)
    {
        // default proc caches clear on catalog update
        m_defaultProcCache.clear();

        m_defaultProcManager = catalogContext.m_defaultProcs;
        m_csp = csp;
        m_plannerTool = catalogContext.m_ptool;
        m_registeredSysProcPlanFragments.clear();
        ImmutableMap.Builder<String, ProcedureRunner> builder =
                loadProceduresFromCatalog(catalogContext, backendTarget);
        loadSystemProcedures(catalogContext, backendTarget, builder);
        procs = builder.build();

    }

    private ImmutableMap.Builder<String, ProcedureRunner> loadProceduresFromCatalog(
            CatalogContext catalogContext,
            BackendTarget backendTarget) {
        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = catalogContext.database.getProcedures();
        ImmutableMap.Builder<String, ProcedureRunner> builder = ImmutableMap.<String, ProcedureRunner>builder();
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

                Language lang;
                try {
                    lang = Language.valueOf(proc.getLanguage());
                } catch (IllegalArgumentException e) {
                    // default to java for earlier compiled catalogs
                    lang = Language.JAVA;
                }

                Class<?> procClass = null;
                try {
                    procClass = catalogContext.classForProcedure(className);
                }
                catch (final ClassNotFoundException e) {
                    if (className.startsWith("org.voltdb.")) {
                        VoltDB.crashLocalVoltDB("VoltDB does not support procedures with package names " +
                                                        "that are prefixed with \"org.voltdb\". Please use a different " +
                                                        "package name and retry. Procedure name was " + className + ".",
                                                        false, null);
                    }
                    else {
                        VoltDB.crashLocalVoltDB("VoltDB was unable to load a procedure (" +
                                                 className + ") it expected to be in the " +
                                                "catalog jarfile and will now exit.", false, null);
                    }
                }
                try {
                    procedure = lang.accept(procedureInstantiator, procClass);
                }
                catch (final Exception e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { m_siteId, m_siteIndex }, e);
                }
            }
            else {
                procedure = new ProcedureRunner.StmtProcedure();
            }

            assert(procedure != null);
            runner = m_runnerFactory.create(procedure, proc, m_csp);
            builder.put(proc.getTypeName().intern(), runner);
        }
        return builder;
    }

    private static Language.CheckedExceptionVisitor<VoltProcedure, Class<?>, Exception> procedureInstantiator =
            new Language.CheckedExceptionVisitor<VoltProcedure, Class<?>, Exception>() {
                @Override
                public VoltProcedure visitJava(Class<?> p) throws Exception {
                    return (VoltProcedure)p.newInstance();
                }
                @Override
                public VoltProcedure visitGroovy(Class<?> p) throws Exception {
                    return new GroovyScriptProcedureDelegate(p);
                }
            };

    private void loadSystemProcedures(
            CatalogContext catalogContext,
            BackendTarget backendTarget,
            ImmutableMap.Builder<String, ProcedureRunner> builder) {
        Set<Entry<String,Config>> entrySet = SystemProcedureCatalog.listing.entrySet();
        for (Entry<String, Config> entry : entrySet) {
            Config sysProc = entry.getValue();
            Procedure proc = sysProc.asCatalogProcedure();

            VoltSystemProcedure procedure = null;
            ProcedureRunner runner = null;

            final String className = sysProc.getClassname();
            Class<?> procClass = null;

            // this check is for sysprocs that don't have a procedure class
            if (className != null) {
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

                runner = m_runnerFactory.create(procedure, proc, m_csp);
                procedure.initSysProc(m_site, this, proc, catalogContext.cluster);
                builder.put(entry.getKey().intern(), runner);
            }
        }
    }

    public ProcedureRunner getProcByName(String procName)
    {
        // Check the procs from the catalog
        ProcedureRunner pr = procs.get(procName);

        // if not there, check the default proc cache
        if (pr == null) {
            pr = m_defaultProcCache.get(procName);
        }

        // if not in the cache, compile the full default proc and put it in the cache
        if (pr == null) {
            Procedure catProc = m_defaultProcManager.checkForDefaultProcedure(procName);
            if (catProc != null) {
                String sqlText = m_defaultProcManager.sqlForDefaultProc(catProc);
                Procedure newCatProc = StatementCompiler.compileDefaultProcedure(m_plannerTool, catProc, sqlText);
                VoltProcedure voltProc = new ProcedureRunner.StmtProcedure();
                pr = m_runnerFactory.create(voltProc, newCatProc, m_csp);
                // this will ensure any created fragment tasks know to load the plans
                // for this plan-on-the-fly procedure
                pr.setProcNameToLoadForFragmentTasks(catProc.getTypeName());
                m_defaultProcCache.put(procName, pr);
            }
        }

        // return what we got, hopefully not null
        return pr;
    }
}
