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

package org.voltdb.compiler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.TreeSet;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Charsets;

/**
 * Compiles individual SQL statements and updates the given catalog.
 * <br/>Invokes the Optimizer to generate plans.
 *
 */
public abstract class StatementCompiler {

    public static final int DEFAULT_MAX_JOIN_TABLES = 5;
    private static VoltLogger m_logger = new VoltLogger("COMPILER");

    /**
     * This static method conveniently does a few things for its caller:
     * - Formats the statement by replacing newlines with spaces
     *     and appends a semicolon if needed
     * - Updates the catalog Statement with metadata about the statement
     * - Plans the statement and puts the serialized plan in the catalog Statement
     * - Updates the catalog Statment with info about the statement's parameters
     * Upon successful completion, catalog statement will have been updated with
     * plan fragments needed to execute the statement.
     *
     * @param  compiler     The VoltCompiler instance
     * @param  hsql         Pass through parameter to QueryPlanner
     * @param  catalog      Pass through parameter to QueryPlanner
     * @param  db           Pass through parameter to QueryPlanner
     * @param  estimates    Pass through parameter to QueryPlanner
     * @param  catalogStmt  Catalog statement to be updated with plan
     * @param  xml          XML for statement, if it has been previously parsed
     *                      (may be null)
     * @param  stmt         Text of statement to be compiled
     * @param  joinOrder    Pass through parameter to QueryPlanner
     * @param  detMode      Pass through parameter to QueryPlanner
     * @param  partitioning Partition info for statement
     * @param  isForView    {@code true} if this statement is being planned for a view
    */
    static boolean compileStatementAndUpdateCatalog(VoltCompiler compiler, HSQLInterface hsql,
            Database db, DatabaseEstimates estimates,
            Statement catalogStmt, VoltXMLElement xml, String stmt, String joinOrder,
            DeterminismMode detMode, StatementPartitioning partitioning, boolean isForView)
    throws VoltCompiler.VoltCompilerException {

        // Cleanup whitespace newlines for catalog compatibility
        // and to make statement parsing easier.
        stmt = stmt.replaceAll("\n", " ");
        stmt = stmt.trim();
        compiler.addInfo("Compiling Statement: " + stmt);

        // put the data in the catalog that we have
        if (!stmt.endsWith(";")) {
            stmt += ";";
        }

        // if this key + sql is the same, then a cached stmt can be used
        String keyPrefix = compiler.getKeyPrefix(partitioning, detMode, joinOrder);
        // if the key is cache-able, look for a previous statement
        if (keyPrefix != null) {
            Statement previousStatement = compiler.getCachedStatement(keyPrefix, stmt);
            // check if the stmt exists and if it's the same sql text
            if (previousStatement != null) {
                if (m_logger.isDebugEnabled()) {
                    Procedure prevproc = (Procedure)previousStatement.getParent();
                    Procedure currproc = (Procedure)catalogStmt.getParent();
                    m_logger.debug(String.format("Recovering statement %s.%s from statement %s.%s\n",
                                                 currproc.getTypeName(),
                                                 catalogStmt.getTypeName(),
                                                 prevproc.getTypeName(),
                                                 previousStatement.getTypeName()));
                }
                catalogStmt.setAnnotation(previousStatement.getAnnotation());
                catalogStmt.setAttachment(previousStatement.getAttachment());
                catalogStmt.setCachekeyprefix(previousStatement.getCachekeyprefix());
                catalogStmt.setCost(previousStatement.getCost());
                catalogStmt.setExplainplan(previousStatement.getExplainplan());
                catalogStmt.setIscontentdeterministic(previousStatement.getIscontentdeterministic());
                catalogStmt.setIsorderdeterministic(previousStatement.getIsorderdeterministic());
                catalogStmt.setNondeterminismdetail(previousStatement.getNondeterminismdetail());
                catalogStmt.setQuerytype(previousStatement.getQuerytype());
                catalogStmt.setReadonly(previousStatement.getReadonly());
                catalogStmt.setReplicatedtabledml(previousStatement.getReplicatedtabledml());
                catalogStmt.setSeqscancount(previousStatement.getSeqscancount());
                catalogStmt.setSinglepartition(previousStatement.getSinglepartition());
                catalogStmt.setSqltext(previousStatement.getSqltext());
                catalogStmt.setTablesread(previousStatement.getTablesread());
                catalogStmt.setTablesupdated(previousStatement.getTablesupdated());
                catalogStmt.setIndexesused(previousStatement.getIndexesused());
                copyUDFDependees(compiler, catalogStmt, previousStatement, db.getFunctions());
                for (StmtParameter oldSp : previousStatement.getParameters()) {
                    StmtParameter newSp = catalogStmt.getParameters().add(oldSp.getTypeName());
                    newSp.setAnnotation(oldSp.getAnnotation());
                    newSp.setAttachment(oldSp.getAttachment());
                    newSp.setIndex(oldSp.getIndex());
                    newSp.setIsarray(oldSp.getIsarray());
                    newSp.setJavatype(oldSp.getJavatype());
                    newSp.setSqltype(oldSp.getSqltype());
                }

                for (PlanFragment oldFrag : previousStatement.getFragments()) {
                    PlanFragment newFrag = catalogStmt.getFragments().add(oldFrag.getTypeName());
                    newFrag.setAnnotation(oldFrag.getAnnotation());
                    newFrag.setAttachment(oldFrag.getAttachment());
                    newFrag.setHasdependencies(oldFrag.getHasdependencies());
                    newFrag.setMultipartition(oldFrag.getMultipartition());
                    newFrag.setNontransactional(oldFrag.getNontransactional());
                    newFrag.setPlanhash(oldFrag.getPlanhash());
                    newFrag.setPlannodetree(oldFrag.getPlannodetree());
                }

                return true;
            }
        }


        if (m_logger.isDebugEnabled()) {
            m_logger.debug(String.format("Compiling %s.%s: sql = \"%s\"\n",
                                         catalogStmt.getParent().getTypeName(),
                                         catalogStmt.getTypeName(),
                                         catalogStmt.getSqltext()));
        }

        // determine the type of the query
        QueryType qtype = QueryType.getFromSQL(stmt);

        catalogStmt.setReadonly(qtype.isReadOnly());
        catalogStmt.setQuerytype(qtype.getValue());

        // might be null if not cacheable
        catalogStmt.setCachekeyprefix(keyPrefix);


        catalogStmt.setSqltext(stmt);
        catalogStmt.setSinglepartition(partitioning.wasSpecifiedAsSingle());

        String name = catalogStmt.getParent().getTypeName() + "-" + catalogStmt.getTypeName();
        String sql = catalogStmt.getSqltext();
        String stmtName = catalogStmt.getTypeName();
        String procName = catalogStmt.getParent().getTypeName();
        TrivialCostModel costModel = new TrivialCostModel();

        CompiledPlan plan = null;

        try {
            // This try-with-resources block acquires a global lock on all planning
            // This is required until we figure out how to do parallel planning.
            try (QueryPlanner planner = new QueryPlanner(
                    sql, stmtName, procName,  db,
                    partitioning, hsql, estimates, false,
                    costModel, null, joinOrder, detMode, false, isForView)) {
                if (xml != null) {
                    planner.parseFromXml(xml);
                }
                else {
                    planner.parse();
                }

                plan = planner.plan();
                assert(plan != null);
            }
            catch (Exception e) {
                // These are normal expectable errors -- don't normally need a stack-trace.
                String msg = "Failed to plan for statement (" + catalogStmt.getTypeName() + ") \"" +
                        catalogStmt.getSqltext() + "\".";
                if (e.getMessage() != null) {
                    msg += " Error: \"" + e.getMessage() + "\"";
                }
                throw compiler.new VoltCompilerException(msg);
            }

            // There is a hard-coded limit to the number of parameters that can be passed to the EE.
            if (plan.getParameters().length > CompiledPlan.MAX_PARAM_COUNT) {
                throw compiler.new VoltCompilerException(
                    "The statement's parameter count " + plan.getParameters().length +
                    " must not exceed the maximum " + CompiledPlan.MAX_PARAM_COUNT);
            }

            // Check order and content determinism before accessing the detail which
            // it caches.
            boolean orderDeterministic = plan.isOrderDeterministic();
            catalogStmt.setIsorderdeterministic(orderDeterministic);
            boolean contentDeterministic = plan.isContentDeterministic()
                                           && (orderDeterministic || !plan.hasLimitOrOffset());
            catalogStmt.setIscontentdeterministic(contentDeterministic);
            String nondeterminismDetail = plan.nondeterminismDetail();
            catalogStmt.setNondeterminismdetail(nondeterminismDetail);

            catalogStmt.setSeqscancount(plan.countSeqScans());

            // Input Parameters
            // We will need to update the system catalogs with this new information
            for (int i = 0; i < plan.getParameters().length; ++i) {
                StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(i));
                catalogParam.setJavatype(plan.getParameters()[i].getValueType().getValue());
                catalogParam.setIsarray(plan.getParameters()[i].getParamIsVector());
                catalogParam.setIndex(i);
            }

            catalogStmt.setReplicatedtabledml(plan.replicatedTableDML);

            // output the explained plan to disk (or caller) for debugging
            StringBuilder planDescription = new StringBuilder(1000); // Initial capacity estimate.
            planDescription.append("SQL: ").append(plan.sql);
            // Only output the cost in debug mode.
            // Cost seems to only confuse people who don't understand how this number is used/generated.
            if (VoltCompiler.DEBUG_MODE) {
                planDescription.append("\nCOST: ").append(plan.cost);
            }
            planDescription.append("\nPLAN:\n");
            planDescription.append(plan.explainedPlan);
            String planString = planDescription.toString();
            // only write to disk if compiler is in standalone mode
            if (compiler.standaloneCompiler) {
                BuildDirectoryUtils.writeFile(null, name + ".txt", planString, false);
            }
            compiler.captureDiagnosticContext(planString);

            // build usage links for report generation and put them in the catalog
            CatalogUtil.updateUsageAnnotations(db, catalogStmt, plan.rootPlanGraph, plan.subPlanGraph);

            // set the explain plan output into the catalog (in hex) for reporting
            catalogStmt.setExplainplan(Encoder.hexEncode(plan.explainedPlan));

            // Add the UDF dependences.
            CatalogMap<Function> functions = db.getFunctions();
            for (String dependee : plan.getUDFDependees()) {
                Function function = functions.get(dependee);
                assert(function != null);
                addUDFDependences(function, catalogStmt);
            }
            // compute a hash of the plan
            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("SHA-1");
            }
            catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                assert(false);
                System.exit(-1); // should never happen with healthy jvm
            }

            // Now update our catalog information
            PlanFragment planFragment = catalogStmt.getFragments().add("0");
            planFragment.setHasdependencies(plan.subPlanGraph != null);
            // mark a fragment as non-transactional if it never touches a persistent table
            planFragment.setNontransactional(!fragmentReferencesPersistentTable(plan.rootPlanGraph));
            planFragment.setMultipartition(plan.subPlanGraph != null);
            byte[] planBytes = writePlanBytes(compiler, planFragment, plan.rootPlanGraph);
            md.update(planBytes, 0, planBytes.length);
            // compute the 40 bytes of hex from the 20 byte sha1 hash of the plans
            md.reset();
            md.update(planBytes);
            planFragment.setPlanhash(Encoder.hexEncode(md.digest()));

            if (plan.subPlanGraph != null) {
                planFragment = catalogStmt.getFragments().add("1");
                planFragment.setHasdependencies(false);
                planFragment.setNontransactional(false);
                planFragment.setMultipartition(true);
                byte[] subBytes = writePlanBytes(compiler, planFragment, plan.subPlanGraph);
                // compute the 40 bytes of hex from the 20 byte sha1 hash of the plans
                md.reset();
                md.update(subBytes);
                planFragment.setPlanhash(Encoder.hexEncode(md.digest()));
            }

            // Planner should have rejected with an exception any statement with an unrecognized type.
            int validType = catalogStmt.getQuerytype();
            assert(validType != QueryType.INVALID.getValue());

            return false;
        }
        catch (StackOverflowError error) {
            String msg = "Failed to plan for statement (" + catalogStmt.getTypeName() + ") \"" +
                    catalogStmt.getSqltext() + "\". Error: \"Encountered stack overflow error. " +
                    "Try reducing the number of predicate expressions in the query.\"";
            throw compiler.new VoltCompilerException(msg);
        }
    }

    private static void copyUDFDependees(VoltCompiler compiler,
                                         Statement catalogStmt,
                                         Statement previousStatement,
                                         CatalogMap<Function> functions) throws VoltCompilerException {
        /*
         * The function dependees are just the names
         * of the UDFs on which the previous statement depends.
         */
        for (String previousDependee : previousStatement.getFunctiondependees().split(",")) {
            if ( ! previousDependee.isEmpty()) {
                Function function = functions.get(previousDependee);
                if (function == null) {
                    Procedure procedure = (Procedure)catalogStmt.getParent();
                    // We must have dropped the function.  Since this statement
                    // depends on it, we have to abandon this effort.
                    throw compiler.new VoltCompilerException(String.format(
                            "Cannot drop user defined function \"%s\".  The statement %s.%s depends on it.",
                            previousDependee,
                            procedure.getTypeName(),
                            catalogStmt.getTypeName()));
                }
                addUDFDependences(function, catalogStmt);
            }
        }
    }


    /**
     * Add all statement dependences, both ways.
     * @param function The function to add as dependee.
     * @param procedure The procedure of the statement.
     * @param catalogStmt The statement to add as depender.
     */
    private static void addUDFDependences(Function function, Statement catalogStmt) {
        Procedure procedure = (Procedure)catalogStmt.getParent();
        addFunctionDependence(function, procedure, catalogStmt);
        addStatementDependence(function, catalogStmt);
    }

    /**
     * Add a dependence to a function of a statement.  The function's
     * dependence string is altered with this function.
     *
     * @param function The function to add as dependee.
     * @param procedure The procedure of the statement.
     * @param catalogStmt The statement to add as depender.
     */
    private static void addFunctionDependence(Function function, Procedure procedure, Statement catalogStmt) {
        String funcDeps = function.getStmtdependers();
        Set<String> stmtSet = new TreeSet<>();
        for (String stmtName : funcDeps.split(",")) {
            if (! stmtName.isEmpty()) {
                stmtSet.add(stmtName);
            }
        }

        String statementName = procedure.getTypeName() + ":" + catalogStmt.getTypeName();
        if (stmtSet.contains(statementName)) {
            return;
        }

        stmtSet.add(statementName);
        StringBuilder sb = new StringBuilder();
        // We will add this procedure:statement pair.  So make sure we have
        // an initial comma.  Note that an empty set must be represented
        // by an empty string.  We represent the set {pp:ss, qq:tt},
        // where "pp" and "qq" are procedures and "ss" and "tt" are
        // statements in their procedures respectively, with
        // the string ",pp:ss,qq:tt,".  If we search for "pp:ss" we will
        // never find "ppp:sss" by accident.
        //
        // Do to this, when we add something to string we start with a single
        // comma, and then add "qq:tt," at the end.
        sb.append(",");
        for (String stmtName : stmtSet) {
            sb.append(stmtName + ",");
        }

        function.setStmtdependers(sb.toString());
    }

    /**
     * Add a dependence of a statement to a function.  The statement's
     * dependence string is altered with this function.
     *
     * @param function The function to add as dependee.
     * @param catalogStmt The statement to add as depender.
     */
    private static void addStatementDependence(Function function, Statement catalogStmt) {
        String fnDeps = catalogStmt.getFunctiondependees();
        Set<String> fnSet = new TreeSet<>();
        for (String fnName : fnDeps.split(",")) {
            if (! fnName.isEmpty()) {
                fnSet.add(fnName);
            }
        }

        String functionName = function.getTypeName();
        if (fnSet.contains(functionName)) {
            return;
        }

        fnSet.add(functionName);
        StringBuilder sb = new StringBuilder();
        sb.append(",");
        for (String fnName : fnSet) {
            sb.append(fnName + ",");
        }

        catalogStmt.setFunctiondependees(sb.toString());
    }

    static boolean compileFromSqlTextAndUpdateCatalog(VoltCompiler compiler, HSQLInterface hsql,
            Database db, DatabaseEstimates estimates,
            Statement catalogStmt, String sqlText, String joinOrder,
            DeterminismMode detMode, StatementPartitioning partitioning)
    throws VoltCompiler.VoltCompilerException {
        return compileStatementAndUpdateCatalog(compiler, hsql, db, estimates, catalogStmt,
                null, sqlText, joinOrder, detMode, partitioning, false);
    }

    /**
     * Update the plan fragment and return the bytes of the plan
     */
    static byte[] writePlanBytes(VoltCompiler compiler, PlanFragment fragment, AbstractPlanNode planGraph)
    throws VoltCompilerException {
        String json = null;
        // get the plan bytes
        PlanNodeList node_list = new PlanNodeList(planGraph, false);
        json = node_list.toJSONString();
        compiler.captureDiagnosticJsonFragment(json);
        // Place serialized version of PlanNodeTree into a PlanFragment
        byte[] jsonBytes = json.getBytes(Charsets.UTF_8);
        String bin64String = CompressionService.compressAndBase64Encode(jsonBytes);
        fragment.setPlannodetree(bin64String);
        return jsonBytes;
    }

    /**
     * Check through a plan graph and return true if it ever touches a persistent table.
     */
    static boolean fragmentReferencesPersistentTable(AbstractPlanNode node) {
        if (node == null)
            return false;

        // these nodes can read/modify persistent tables
        if (node instanceof AbstractScanPlanNode)
            return true;
        if (node instanceof InsertPlanNode)
            return true;
        if (node instanceof DeletePlanNode)
            return true;
        if (node instanceof UpdatePlanNode)
            return true;

        // recursively check out children
        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            if (fragmentReferencesPersistentTable(child))
                return true;
        }

        // if nothing found, return false
        return false;
    }

    /**
     * This procedure compiles a shim org.voltdb.catalog.Procedure representing a default proc.
     * The shim has no plan and few details that are expensive to compute.
     * The returned proc instance has a full plan and can be used to create a ProcedureRunner.
     * Note that while there are two procedure objects here, none are rooted in a real catalog;
     * they are entirely parallel to regular, catalog procs.
     *
     * This code could probably go a few different places. It duplicates a bit too much of the
     * StatmentCompiler code for my taste, so I put it here. Next pass could reduce some of the
     * duplication?
     */
    public static Procedure compileDefaultProcedure(PlannerTool plannerTool, Procedure catProc, String sqlText) {
        // fake db makes it easy to create procedures that aren't part of the main catalog
        Database fakeDb = new Catalog().getClusters().add("cluster").getDatabases().add("database");

        Table table = catProc.getPartitiontable();

        // determine the type of the query
        QueryType qtype = QueryType.getFromSQL(sqlText);

        StatementPartitioning partitioning =
                catProc.getSinglepartition() ? StatementPartitioning.forceSP() :
                                               StatementPartitioning.forceMP();

        CompiledPlan plan = plannerTool.planSqlCore(sqlText, partitioning);


        Procedure newCatProc = fakeDb.getProcedures().add(catProc.getTypeName());
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

        /* since there can be multiple statements in a procedure,
         * we name the statements starting from 'sql0' even for single statement procedures
         * since we reuse the same code for single and multi-statement procedures
         *     statements of all single statement procedures are named 'sql0'
        */
        Statement stmt = statements.add(VoltDB.ANON_STMT_NAME + "0");
        stmt.setSqltext(sqlText);
        stmt.setReadonly(catProc.getReadonly());
        stmt.setQuerytype(qtype.getValue());
        stmt.setSinglepartition(catProc.getSinglepartition());
        stmt.setIscontentdeterministic(true);
        stmt.setIsorderdeterministic(true);
        stmt.setNondeterminismdetail("NO CONTENT FOR DEFAULT PROCS");
        stmt.setSeqscancount(plan.countSeqScans());
        stmt.setReplicatedtabledml(!catProc.getReadonly() && table.getIsreplicated());

        // Input Parameters
        // We will need to update the system catalogs with this new information
        for (int i = 0; i < plan.getParameters().length; ++i) {
            StmtParameter catalogParam = stmt.getParameters().add(String.valueOf(i));
            catalogParam.setIndex(i);
            ParameterValueExpression pve = plan.getParameters()[i];
            catalogParam.setJavatype(pve.getValueType().getValue());
            catalogParam.setIsarray(pve.getParamIsVector());
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

        return newCatProc;
    }

    /**
     * Update the plan fragment and return the bytes of the plan
     */
    static byte[] writePlanBytes(PlanFragment fragment, AbstractPlanNode planGraph) {
        // get the plan bytes
        PlanNodeList node_list = new PlanNodeList(planGraph, false);
        String json = node_list.toJSONString();
        // Place serialized version of PlanNodeTree into a PlanFragment
        byte[] jsonBytes = json.getBytes(Charsets.UTF_8);
        String bin64String = CompressionService.compressAndBase64Encode(jsonBytes);
        fragment.setPlannodetree(bin64String);
        return jsonBytes;
    }

    private static String genSelectSqlForNibbleDelete(Table table, Column column,
            ComparisonOperation comparison) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM " + table.getTypeName());
        sb.append(" WHERE " + column.getName() + " " + comparison.toString() + " ?;");
        return sb.toString();
    }

    private static String genDeleteSqlForNibbleDelete(Table table, Column column,
            ComparisonOperation comparison) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM " + table.getTypeName());
        sb.append(" WHERE " + column.getName() + " " + comparison.toString() + " ?;");
        return sb.toString();
    }

    private static String genValueAtOffsetSqlForNibbleDelete(Table table, Column column,
            ComparisonOperation comparison) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT " + column.getName() + " FROM " + table.getTypeName());
        sb.append(" ORDER BY " + column.getName());
        if (comparison == ComparisonOperation.LTE || comparison == ComparisonOperation.LT) {
            sb.append(" ASC OFFSET ? LIMIT 1;");
        } else {
            sb.append(" DESC OFFSET ? LIMIT 1;");
        }
        return sb.toString();
    }

    private static Procedure addProcedure(Table catTable, String procName) {
        // fake db makes it easy to create procedures that aren't part of the main catalog
        Database fakeDb = new Catalog().getClusters().add("cluster").getDatabases().add("database");
        Column partitionColumn = catTable.getPartitioncolumn();
        Procedure newCatProc = fakeDb.getProcedures().add(procName);
        newCatProc.setClassname(procName);
        newCatProc.setDefaultproc(false);
        newCatProc.setEverysite(false);
        newCatProc.setHasjava(false);
        newCatProc.setPartitioncolumn(catTable.getPartitioncolumn());
        if (catTable.getIsreplicated()) {
            newCatProc.setPartitionparameter(-1);
        } else {
            newCatProc.setPartitionparameter(partitionColumn.getIndex());
        }
        newCatProc.setPartitiontable(catTable);
        newCatProc.setReadonly(false);
        newCatProc.setSinglepartition(!catTable.getIsreplicated());
        newCatProc.setSystemproc(false);
        if (!catTable.getIsreplicated()) {
            newCatProc.setAttachment(
                    new ProcedurePartitionInfo(
                            VoltType.get((byte)partitionColumn.getType()),
                            partitionColumn.getIndex()));
        }

        return newCatProc;
    }

    private static void addStatement(Table catTable, Procedure newCatProc, String sqlText, String index) {
        CatalogMap<Statement> statements = newCatProc.getStatements();
        assert(statements != null);

        // determine the type of the query
        QueryType qtype = QueryType.getFromSQL(sqlText);

        CatalogContext context = VoltDB.instance().getCatalogContext();
        PlannerTool plannerTool = context.m_ptool;

        StatementPartitioning partitioning =
                newCatProc.getSinglepartition() ? StatementPartitioning.forceSP() :
                                               StatementPartitioning.forceMP();

        CompiledPlan plan = plannerTool.planSqlCore(sqlText, partitioning);
        /* since there can be multiple statements in a procedure,
         * we name the statements starting from 'sql0' even for single statement procedures
         * since we reuse the same code for single and multi-statement procedures
         *     statements of all single statement procedures are named 'sql0'
        */
        Statement stmt = statements.add(VoltDB.ANON_STMT_NAME + index);
        stmt.setSqltext(sqlText);
        stmt.setReadonly(newCatProc.getReadonly());
        stmt.setQuerytype(qtype.getValue());
        stmt.setSinglepartition(newCatProc.getSinglepartition());
        stmt.setIscontentdeterministic(true);
        stmt.setIsorderdeterministic(true);
        stmt.setNondeterminismdetail("NO CONTENT FOR DEFAULT PROCS");
        stmt.setSeqscancount(plan.countSeqScans());
        stmt.setReplicatedtabledml(!newCatProc.getReadonly() && catTable.getIsreplicated());

        // Input Parameters
        // We will need to update the system catalogs with this new information
        for (int i = 0; i < plan.getParameters().length; ++i) {
            StmtParameter catalogParam = stmt.getParameters().add(String.valueOf(i));
            catalogParam.setIndex(i);
            ParameterValueExpression pve = plan.getParameters()[i];
            catalogParam.setJavatype(pve.getValueType().getValue());
            catalogParam.setIsarray(pve.getParamIsVector());
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
    }

    /**
     * Generate small deletion queries by using count - select - delete pattern.
     *
     * 1) First query finds number of rows meet the delete condition.
     * 2) Second query finds the cut-off value if number of rows to be deleted is
     *    higher than maximum delete chunk size.
     * 3) Third query deletes rows selected by above queries.
     */
    public static Procedure compileNibbleDeleteProcedure(Table catTable, String procName,
            Column col, ComparisonOperation comp) {
        Procedure newCatProc = addProcedure(catTable, procName);

        String countingQuery = genSelectSqlForNibbleDelete(catTable, col, comp);
        addStatement(catTable, newCatProc, countingQuery, "0");

        String deleteQuery = genDeleteSqlForNibbleDelete(catTable, col, comp);
        addStatement(catTable, newCatProc, deleteQuery, "1");

        String valueAtQuery = genValueAtOffsetSqlForNibbleDelete(catTable, col, comp);
        addStatement(catTable, newCatProc, valueAtQuery, "2");

        return newCatProc;
    }

    /**
     * Generate migrate queries by using count - select - migrate pattern.
     *
     * 1) First query finds number of rows meet the migrate condition.
     * 2) Second query finds the cut-off value if number of rows to be migrate is
     *    higher than maximum migrate chunk size.
     * 3) Third query migrates rows selected by above queries.
     */
    public static Procedure compileMigrateProcedure(Table table, String procName,
            Column column, ComparisonOperation comparison) {
        Procedure proc = addProcedure(table, procName);

        // Select count(*)
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM " + table.getTypeName());
        sb.append(" WHERE not migrating AND " + column.getName() + " " + comparison.toString() + " ?;");
        addStatement(table, proc, sb.toString(), "0");

         // Get cutoff value
        sb.setLength(0);
        sb.append("SELECT " + column.getName() + " FROM " + table.getTypeName());
        sb.append(" WHERE not migrating ORDER BY " + column.getName());
        if (comparison == ComparisonOperation.LTE || comparison == ComparisonOperation.LT) {
            sb.append(" ASC OFFSET ? LIMIT 1;");
        } else {
            sb.append(" DESC OFFSET ? LIMIT 1;");
        }
        addStatement(table, proc, sb.toString(), "1");

        // Migrate
        sb.setLength(0);
        sb.append("MIGRATE FROM " + table.getTypeName());
        sb.append(" WHERE not migrating AND " + column.getName() + " " + comparison.toString() + " ?;");
        addStatement(table, proc, sb.toString(), "2");

        return proc;
    }
}
