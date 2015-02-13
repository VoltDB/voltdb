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

package org.voltdb.compiler;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Charsets;

/**
 * Compiles individual SQL statements and updates the given catalog.
 * <br/>Invokes the Optimizer to generate plans.
 *
 */
public abstract class StatementCompiler {

    public static final int DEFAULT_MAX_JOIN_TABLES = 5;

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
    */
    static boolean compileStatementAndUpdateCatalog(VoltCompiler compiler, HSQLInterface hsql,
            Catalog catalog, Database db, DatabaseEstimates estimates,
            Statement catalogStmt, VoltXMLElement xml, String stmt, String joinOrder,
            DeterminismMode detMode, StatementPartitioning partitioning)
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
        QueryPlanner planner = new QueryPlanner(
                sql, stmtName, procName,  catalog.getClusters().get("cluster"), db,
                partitioning, hsql, estimates, false, DEFAULT_MAX_JOIN_TABLES,
                costModel, null, joinOrder, detMode);
        try {
            if (xml != null) {
                planner.parseFromXml(xml);
            }
            else {
                planner.parse();
            }

            plan = planner.plan();
            assert(plan != null);
        } catch (PlanningErrorException e) {
            // These are normal expectable errors -- don't normally need a stack-trace.
            String msg = "Failed to plan for statement (" + catalogStmt.getTypeName() + ") " + catalogStmt.getSqltext();
            if (e.getMessage() != null) {
                msg += " Error: \"" + e.getMessage() + "\"";
            }
            throw compiler.new VoltCompilerException(msg);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw compiler.new VoltCompilerException("Failed to plan for stmt: " + catalogStmt.getTypeName());
        }

        // There is a hard-coded limit to the number of parameters that can be passed to the EE.
        if (plan.parameters.length > CompiledPlan.MAX_PARAM_COUNT) {
            throw compiler.new VoltCompilerException(
                "The statement's parameter count " + plan.parameters.length +
                " must not exceed the maximum " + CompiledPlan.MAX_PARAM_COUNT);
        }

        // Check order determinism before accessing the detail which it caches.
        boolean orderDeterministic = plan.isOrderDeterministic();
        catalogStmt.setIsorderdeterministic(orderDeterministic);
        boolean contentDeterministic = orderDeterministic || ! plan.hasLimitOrOffset();
        catalogStmt.setIscontentdeterministic(contentDeterministic);
        String nondeterminismDetail = plan.nondeterminismDetail();
        catalogStmt.setNondeterminismdetail(nondeterminismDetail);

        catalogStmt.setSeqscancount(plan.countSeqScans());

        // Input Parameters
        // We will need to update the system catalogs with this new information
        for (int i = 0; i < plan.parameters.length; ++i) {
            StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(i));
            catalogParam.setJavatype(plan.parameters[i].getValueType().getValue());
            catalogParam.setIsarray(plan.parameters[i].getParamIsVector());
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

        // compute a hash of the plan
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
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

    static boolean compileFromSqlTextAndUpdateCatalog(VoltCompiler compiler, HSQLInterface hsql,
            Catalog catalog, Database db, DatabaseEstimates estimates,
            Statement catalogStmt, String sqlText, String joinOrder,
            DeterminismMode detMode, StatementPartitioning partitioning)
    throws VoltCompiler.VoltCompilerException {
        return compileStatementAndUpdateCatalog(compiler, hsql, catalog, db, estimates, catalogStmt,
                null, sqlText, joinOrder, detMode, partitioning);
    }

    /**
     * Update the plan fragment and return the bytes of the plan
     */
    static byte[] writePlanBytes(VoltCompiler compiler, PlanFragment fragment, AbstractPlanNode planGraph)
    throws VoltCompilerException {
        // get the plan bytes
        PlanNodeList node_list = new PlanNodeList(planGraph);
        String json = node_list.toJSONString();
        compiler.captureDiagnosticJsonFragment(json);
        // Place serialized version of PlanNodeTree into a PlanFragment
        byte[] jsonBytes = json.getBytes(Charsets.UTF_8);
        String bin64String = Encoder.compressAndBase64Encode(jsonBytes);
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

        Statement stmt = statements.add(VoltDB.ANON_STMT_NAME);
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

        return newCatProc;
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
