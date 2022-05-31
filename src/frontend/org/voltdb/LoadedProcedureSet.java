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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;

public class LoadedProcedureSet {

    public static final String ORGVOLTDB_PROCNAME_ERROR_FMT =
            "VoltDB does not support procedures with package names " +
            "that are prefixed with \"org.voltdb\". Please use a different " +
            "package name and retry. Procedure name was %s.";
    public static final String UNABLETOLOAD_ERROR_FMT =
            "VoltDB was unable to load a procedure (%s) it expected to be " +
            "in the catalog jarfile and will now exit.";

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final SiteProcedureConnection m_site;

    // user procedures.
    ImmutableMap<String, ProcedureRunner> m_userProcs = ImmutableMap.<String, ProcedureRunner>builder().build();

    // system procedures.
    ImmutableMap<String, ProcedureRunner> m_sysProcs = ImmutableMap.<String, ProcedureRunner>builder().build();

    // map of sysproc fragment ids to system procedures.
    final HashMap<Long, ProcedureRunner> m_registeredSysProcPlanFragments = new HashMap<Long, ProcedureRunner>();

    // cached default procs
    Map<String, ProcedureRunner> m_defaultProcCache;
    DefaultProcedureManager m_defaultProcManager;
    PlannerTool m_plannerTool;

    public LoadedProcedureSet(SiteProcedureConnection site) {
        m_site = site;

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
     */
    public void loadProcedures(CatalogContext catalogContext) {
        loadProcedures(catalogContext, true);
    }

    /**
     * Load procedures.
     */
    public void loadProcedures(CatalogContext catalogContext, boolean isInitOrReplay)
    {
        m_defaultProcManager = catalogContext.m_defaultProcs;
        // default proc caches clear on catalog update
        m_defaultProcCache.clear();
        m_plannerTool = catalogContext.m_ptool;

        // reload all system procedures from beginning
        m_sysProcs = loadSystemProcedures(catalogContext, m_site);

        try {
            if (isInitOrReplay) {
                // reload user procedures
                m_userProcs = loadUserProcedureRunners(catalogContext.database.getProcedures(),
                                                       catalogContext.getCatalogJar().getLoader(),
                                                       null,
                                                       m_site);
            } else {
                // When catalog updates, only user procedures needs to be reloaded.
                m_userProcs = catalogContext.getPreparedUserProcedureRunners(m_site);
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error trying to load user procedures: " + e.getMessage());
        }
    }

    public static ImmutableMap<String, ProcedureRunner> loadUserProcedureRunners(
            Iterable<Procedure> catalogProcedures,
            ClassLoader loader,
            ImmutableMap<String, Class<?>> classesMap,
            SiteProcedureConnection site) throws Exception
    {
        ImmutableMap.Builder<String, ProcedureRunner> builder = ImmutableMap.<String, ProcedureRunner>builder();

        for (final Procedure proc : catalogProcedures) {
            // Ignore sysprocs found in catalog.
            if (proc.getTypeName().startsWith("@")) {
                continue;
            }

            // skip non-transactional procs. Those will be handled by LoadedNTProcedureSet
            if (proc.getTransactional() == false) {
                continue;
            }

            VoltProcedure procedure = null;

            if (proc.getHasjava()) {
                final String className = proc.getClassname();
                Class<?> procClass = null;
                if (loader == null) {
                    assert(classesMap != null);
                    procClass = classesMap.get(className);
                } else {
                    try {
                        procClass = CatalogContext.classForProcedureOrUDF(className, loader);
                    } catch (final ClassNotFoundException e) {
                        String msg; // generate a better ClassNotFoundException message
                        if (className.startsWith("org.voltdb.")) {
                            msg = String.format(LoadedProcedureSet.ORGVOLTDB_PROCNAME_ERROR_FMT, className);
                        } else {
                            msg = String.format(LoadedProcedureSet.UNABLETOLOAD_ERROR_FMT, className);
                        }
                        throw new ClassNotFoundException(msg);
                    }
                }

                // create new instance VoltProcedure
                procedure = (VoltProcedure) procClass.newInstance();
            }
            else {
                procedure = new ProcedureRunner.StmtProcedure();
            }

            assert(procedure != null);
            ProcedureRunner runner = new ProcedureRunner(procedure, site, proc);
            builder.put(proc.getTypeName().intern(), runner);
        }
        return builder.build();
    }

    private ImmutableMap<String, ProcedureRunner> loadSystemProcedures(
            CatalogContext catalogContext,
            SiteProcedureConnection site)
    {
        // clean up all the registered system plan fragments before reloading system procedures
        m_registeredSysProcPlanFragments.clear();
        ImmutableMap.Builder<String, ProcedureRunner> builder = ImmutableMap.<String, ProcedureRunner>builder();

        List<Long> durableFragments = Lists.newArrayList();
        List<String> replayableProcs = Lists.newArrayList();
        Set<Entry<String,Config>> entrySet = SystemProcedureCatalog.listing.entrySet();
        for (Entry<String, Config> entry : entrySet) {
            Config sysProc = entry.getValue();
            Procedure proc = sysProc.asCatalogProcedure();

            // NT sysprocs handled by NTProcedureService
            if (!sysProc.transactional) {
                continue;
            }

            VoltSystemProcedure procedure = null;

            final String className = sysProc.getClassname();
            Class<?> procClass = null;

            // this check is for sysprocs that don't have a procedure class
            if (className != null) {
                try {
                    procClass = catalogContext.classForProcedureOrUDF(className);
                }
                catch (final ClassNotFoundException e) {
                    if (sysProc.commercial) {
                        continue;
                    }
                    hostLog.warnFmt(e, "Execution site siteId %s", site.getCorrespondingSiteId());
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }

                try {
                    procedure = (VoltSystemProcedure) procClass.newInstance();
                }
                catch (final InstantiationException | IllegalAccessException e) {
                    hostLog.warnFmt(e, "Execution site siteId %s", site.getCorrespondingSiteId());
                }

                ProcedureRunner runner = new ProcedureRunner(procedure, site, proc);

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
                if (!sysProc.singlePartition  && sysProc.isDurable()) {
                    long[] fragIds =  procedure.getAllowableSysprocFragIdsInTaskLog();
                    if (fragIds != null && fragIds.length > 0) {
                        durableFragments.addAll(Arrays.stream(fragIds).boxed().collect(Collectors.toList()));
                    }
                    if (procedure.allowableSysprocForTaskLog()) {
                        replayableProcs.add("@" + runner.m_procedureName);
                    }
                }
            }
        }
        SystemProcedureCatalog.setupAllowableSysprocFragsInTaskLog(durableFragments, replayableProcs);
        return builder.build();
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
            // nibble delete and migrate have special statements
            if (procName.endsWith(DefaultProcedureManager.NIBBLE_MIGRATE_PROC) ||
                    procName.endsWith(DefaultProcedureManager.NIBBLE_DELETE_PROC)) {
                return pr;
            }
            Procedure catProc = m_defaultProcManager.checkForDefaultProcedure(procName);
            if (catProc != null) {
                String sqlText = DefaultProcedureManager.sqlForDefaultProc(catProc);
                Procedure newCatProc = StatementCompiler.compileDefaultProcedure(
                        m_plannerTool, catProc, sqlText);
                VoltProcedure voltProc = new ProcedureRunner.StmtProcedure();
                pr = new ProcedureRunner(voltProc, m_site, newCatProc);
                // this will ensure any created fragment tasks know to load the plans
                // for this plan-on-the-fly procedure
                pr.setProcNameToLoadForFragmentTasks(catProc.getTypeName());
                m_defaultProcCache.put(procName, pr);
            }
        }

        // return what we got, hopefully not null
        return pr;
    }

    /**
     * (TableName).nibbleDelete is cached in default procedure cache.
     * @param tableName
     * @return
     */
    public ProcedureRunner getNibbleDeleteProc(String procName,
                                               Table catTable,
                                               Column column,
                                               ComparisonOperation op)
    {
        ProcedureRunner pr = m_defaultProcCache.get(procName);
        if (pr == null) {
            Procedure newCatProc =
                    StatementCompiler.compileNibbleDeleteProcedure(
                            catTable, procName, column, op);
            VoltProcedure voltProc = new ProcedureRunner.StmtProcedure();
            pr = new ProcedureRunner(voltProc, m_site, newCatProc);
            // this will ensure any created fragment tasks know to load the plans
            // for this plan-on-the-fly procedure
            pr.setProcNameToLoadForFragmentTasks(newCatProc.getTypeName());
            m_defaultProcCache.put(procName, pr);
            // also list nibble delete into default procedures
            m_defaultProcManager.m_defaultProcMap.put(procName.toLowerCase(), pr.getCatalogProcedure());
        }
        return pr;
    }

    public ProcedureRunner getMigrateProcRunner(String procName, Table catTable, Column column,
            ComparisonOperation op) {
        ProcedureRunner runner = m_defaultProcCache.get(procName);
        if (runner == null) {
            Procedure newCatProc = StatementCompiler.compileMigrateProcedure(
                            catTable, procName, column, op);
            VoltProcedure voltProc = new ProcedureRunner.StmtProcedure();
            runner = new ProcedureRunner(voltProc, m_site, newCatProc);
            runner.setProcNameToLoadForFragmentTasks(newCatProc.getTypeName());
            m_defaultProcCache.put(procName, runner);
            m_defaultProcManager.m_defaultProcMap.put(procName.toLowerCase(), runner.getCatalogProcedure());
        }
        return runner;
    }
}
