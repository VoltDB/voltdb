/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

    final SiteProcedureConnection m_site;

    // user procedures.
    ImmutableMap<String, ProcedureRunner> m_userProcs = ImmutableMap.<String, ProcedureRunner>builder().build();

    // system procedures.
    ImmutableMap<String, ProcedureRunner> m_sysProcs = ImmutableMap.<String, ProcedureRunner>builder().build();

    // map of sysproc fragment ids to system procedures.
    final HashMap<Long, ProcedureRunner> m_registeredSysProcPlanFragments = new HashMap<Long, ProcedureRunner>();

    CatalogSpecificPlanner m_csp;

    // cached default procs
    Map<String, ProcedureRunner> m_defaultProcCache;
    DefaultProcedureManager m_defaultProcManager;
    PlannerTool m_plannerTool;

    public LoadedProcedureSet(SiteProcedureConnection site) {
        m_site = site;

        m_csp = null;
        m_defaultProcCache = new HashMap<>();
        m_defaultProcManager = null;
        m_plannerTool = null;
    }

    public ProcedureRunner getSysproc(long fragmentId) {
        return m_registeredSysProcPlanFragments.get(fragmentId);
    }

    private void registerPlanFragment(final long pfId, final ProcedureRunner proc) {
        assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false);
        m_registeredSysProcPlanFragments.put(pfId, proc);
    }

    /**
     * Load all user procedures and system procedures as new procedures from beginning.
     * @param catalogContext
     * @param csp
     */
    public void loadProcedures(
            CatalogContext catalogContext,
            CatalogSpecificPlanner csp) {
        loadProcedures(catalogContext, csp, false);
    }

    /**
     * Load procedures.
     * If @param forUpdateOnly, it will try to reuse existing loaded procedures
     * as many as possible, other than completely loading procedures from beginning.
     * @param catalogContext
     * @param csp
     * @param forUpdateOnly
     */
    public void loadProcedures(
            CatalogContext catalogContext,
            CatalogSpecificPlanner csp,
            boolean forUpdateOnly)
    {
        m_csp = csp;
        m_defaultProcManager = catalogContext.m_defaultProcs;
        // default proc caches clear on catalog update
        m_defaultProcCache.clear();
        m_plannerTool = catalogContext.m_ptool;

        // reload user procedures
        m_userProcs = loadUserProcedureRunners(catalogContext, m_site, m_csp);

        if (forUpdateOnly) {
            // When catalog updates, only user procedures needs to be reloaded.
            // System procedures can be left without changes.
            reInitSystemProcedureRunners(catalogContext, csp);
        } else {
            // reload all system procedures from beginning
            m_sysProcs = loadSystemProcedures(catalogContext, m_site, csp);
        }
    }

    private static ImmutableMap<String, ProcedureRunner> loadUserProcedureRunners(
            CatalogContext catalogContext,
            SiteProcedureConnection site,
            CatalogSpecificPlanner csp
            ) {
        ImmutableMap.Builder<String, ProcedureRunner> builder = ImmutableMap.<String, ProcedureRunner>builder();

        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = catalogContext.database.getProcedures();

        for (final Procedure proc : catalogProcedures) {
            // Sysprocs used to be in the catalog. Now they aren't. Ignore
            // sysprocs found in old catalog versions. (PRO-365)
            if (proc.getTypeName().startsWith("@")) {
                continue;
            }

            VoltProcedure procedure = null;
            // default to Java
            Language lang = Language.JAVA;
            if (proc.getHasjava()) {
                try {
                    lang = Language.valueOf(proc.getLanguage());
                } catch (IllegalArgumentException e) {
                    // default to Java for earlier compiled catalogs
                }

                final String className = proc.getClassname();
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
                    // TODO: remove the extra meaningless parameter "0"
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { site.getCorrespondingSiteId(), 0}, e);
                }
            }
            else {
                procedure = new ProcedureRunner.StmtProcedure();
                lang = null;
            }

            assert(procedure != null);
            ProcedureRunner runner = new ProcedureRunner(lang, procedure, site, proc, csp);
            builder.put(proc.getTypeName().intern(), runner);
        }
        return builder.build();
    }


    private ImmutableMap<String, ProcedureRunner> loadSystemProcedures(
            CatalogContext catalogContext,
            SiteProcedureConnection site,
            CatalogSpecificPlanner csp) {
        // clean up all the registered system plan fragments before reloading system procedures
        m_registeredSysProcPlanFragments.clear();
        ImmutableMap.Builder<String, ProcedureRunner> builder = ImmutableMap.<String, ProcedureRunner>builder();

        Set<Entry<String,Config>> entrySet = SystemProcedureCatalog.listing.entrySet();
        for (Entry<String, Config> entry : entrySet) {
            Config sysProc = entry.getValue();
            Procedure proc = sysProc.asCatalogProcedure();

            VoltSystemProcedure procedure = null;

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
                            // TODO: remove the extra meaningless parameter "0"
                            new Object[] { site.getCorrespondingSiteId(), 0 },
                            e);
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }

                try {
                    procedure = (VoltSystemProcedure) procClass.newInstance();
                }
                catch (final InstantiationException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                            new Object[] { site.getCorrespondingSiteId(), 0 }, e);
                }
                catch (final IllegalAccessException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                            new Object[] { site.getCorrespondingSiteId(), 0 }, e);
                }

                ProcedureRunner runner = new ProcedureRunner(Language.JAVA, procedure,
                        site, site.getSystemProcedureExecutionContext(),
                        proc, csp);

                procedure.initSysProc(site, catalogContext.cluster,
                        catalogContext.getClusterSettings(),
                        catalogContext.getNodeSettings());

                // register the plan fragments with procedure set
                long[] planFragments = procedure.getPlanFragmentIds();
                assert(planFragments != null);
                for (long pfId: planFragments) {
                    registerPlanFragment(pfId, runner);
                }

                builder.put(entry.getKey().intern(), runner);
            }
        }
        return builder.build();
    }

    public void reInitSystemProcedureRunners(
            CatalogContext catalogContext,
            CatalogSpecificPlanner csp)
    {
        for (Entry<String, ProcedureRunner> entry: m_sysProcs.entrySet()) {
            ProcedureRunner runner = entry.getValue();
            runner.reInitSysProc(catalogContext, csp);
        }
    }

    public ProcedureRunner getProcByName(String procName)
    {
        // Check the procs from the catalog
        ProcedureRunner pr = m_userProcs.get(procName);
        if (pr == null) {
            pr = m_sysProcs.get(procName);
        }

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
                pr = new ProcedureRunner(null, voltProc, m_site, newCatProc, m_csp);
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
