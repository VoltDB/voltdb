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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.Language;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.groovy.GroovyScriptProcedureDelegate;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.types.QueryType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableMap;

public class LoadedProcedureSet {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    Database m_fakeDb = null;

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
        // default proc caches
        m_fakeDb = new Catalog().getClusters().add("cluster").getDatabases().add("database");
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
        ProcedureRunner pr = procs.get(procName);
        if (pr == null) {
            pr = m_defaultProcCache.get(procName);
            if (pr == null) {
                Procedure catProc = m_defaultProcManager.checkForDefaultProcedure(procName);
                if (catProc != null) {
                    String sqlText = m_defaultProcManager.sqlForDefaultProc(catProc);
                    Table table = catProc.getPartitiontable();

                    // determine the type of the query
                    QueryType qtype = QueryType.getFromSQL(sqlText);

                    StatementPartitioning partitioning =
                            catProc.getSinglepartition() ? StatementPartitioning.forceSP() :
                                                           StatementPartitioning.forceMP();

                    CompiledPlan plan = m_plannerTool.planSqlCore(sqlText, partitioning);

                    VoltProcedure voltProc = new ProcedureRunner.StmtProcedure();
                    Procedure newCatProc = m_fakeDb.getProcedures().add(procName);
                    newCatProc.setClassname(catProc.getClassname());
                    newCatProc.setDefaultproc(true);
                    newCatProc.setEverysite(false);
                    newCatProc.setHasjava(false);
                    newCatProc.setPartitioncolumn(catProc.getPartitioncolumn());
                    newCatProc.setPartitionparameter(catProc.getPartitionparameter());
                    newCatProc.setPartitiontable(catProc.getPartitiontable());
                    newCatProc.setReadonly(catProc.getReadonly());
                    newCatProc.setSinglepartition(catProc.getSinglepartition());
                    newCatProc.setSystemproc(false);

                    if (catProc.getPartitionparameter() >= 0) {
                        newCatProc.setAttachment(
                                new ProcedurePartitionInfo(
                                        VoltType.get((byte) catProc.getPartitioncolumn().getType()),
                                        catProc.getPartitionparameter()));
                    }

                    CatalogMap<Statement> statements = newCatProc.getStatements();
                    assert(statements != null);

                    Statement stmt = statements.add(VoltDB.ANON_STMT_NAME);
                    stmt.setSqltext(sqlText);
                    stmt.setReadonly(catProc.getReadonly());
                    stmt.setQuerytype(qtype.getValue());
                    stmt.setSinglepartition(catProc.getSinglepartition());
                    stmt.setBatched(false);
                    stmt.setIscontentdeterministic(true);
                    stmt.setIsorderdeterministic(true);
                    stmt.setNondeterminismdetail("NO CONTENT FOR DEFAULT PROCS");
                    stmt.setSeqscancount(plan.countSeqScans());
                    stmt.setReplicatedtabledml(!catProc.getReadonly() && table.getIsreplicated());
                    stmt.setParamnum(plan.parameters.length);

                    // Input Parameters
                    // We will need to update the system catalogs with this new information
                    for (int i = 0; i < plan.parameters.length; ++i) {
                        StmtParameter catalogParam = stmt.getParameters().add(String.valueOf(i));
                        catalogParam.setJavatype(plan.parameters[i].getValueType().getValue());
                        catalogParam.setIsarray(plan.parameters[i].getParamIsVector());
                        catalogParam.setIndex(i);
                    }

                    PlanFragment frag = stmt.getFragments().add("0");

                    // compute a hash of the plan
                    MessageDigest md = null;
                    try {
                        md = MessageDigest.getInstance("SHA-1");
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                        assert(false);
                        System.exit(-1); // should never happen with healthy jvm
                    }

                    byte[] planBytes = writePlanBytes(frag, plan.rootPlanGraph);
                    md.update(planBytes, 0, planBytes.length);
                    // compute the 40 bytes of hex from the 20 byte sha1 hash of the plans
                    md.reset();
                    md.update(planBytes);
                    frag.setPlanhash(Encoder.hexEncode(md.digest()));

                    if (plan.subPlanGraph != null) {
                        frag.setHasdependencies(true);
                        frag.setNontransactional(true);
                        frag.setMultipartition(true);

                        frag = stmt.getFragments().add("1");
                        frag.setHasdependencies(false);
                        frag.setNontransactional(false);
                        frag.setMultipartition(true);
                        byte[] subBytes = writePlanBytes(frag, plan.subPlanGraph);
                        // compute the 40 bytes of hex from the 20 byte sha1 hash of the plans
                        md.reset();
                        md.update(subBytes);
                        frag.setPlanhash(Encoder.hexEncode(md.digest()));
                    }
                    else {
                        frag.setHasdependencies(false);
                        frag.setNontransactional(false);
                        frag.setMultipartition(false);
                    }

                    // set the procedure parameter types from the statement parameter types
                    int paramCount = 0;
                    for (StmtParameter stmtParam : CatalogUtil.getSortedCatalogItems(stmt.getParameters(), "index")) {
                        // name each parameter "param1", "param2", etc...
                        ProcParameter procParam = newCatProc.getParameters().add("param" + String.valueOf(paramCount));
                        procParam.setIndex(stmtParam.getIndex());
                        procParam.setIsarray(stmtParam.getIsarray());
                        procParam.setType(stmtParam.getJavatype());
                        paramCount++;
                    }

                    ProcedureRunner runner = m_runnerFactory.create(voltProc, newCatProc, m_csp);
                    m_defaultProcCache.put(procName, runner);
                    return runner;
                }
            }
        }
        return pr;
    }

    /**
     * Update the plan fragment and return the bytes of the plan
     */
    static byte[] writePlanBytes(PlanFragment fragment, AbstractPlanNode planGraph) {
        // get the plan bytes
        PlanNodeList node_list = new PlanNodeList(planGraph);
        String json = node_list.toJSONString();
        // Place serialized version of PlanNodeTree into a PlanFragment
        byte[] jsonBytes = json.getBytes(Charsets.UTF_8);
        String bin64String = Encoder.compressAndBase64Encode(jsonBytes);
        fragment.setPlannodetree(bin64String);
        return jsonBytes;
    }
}
