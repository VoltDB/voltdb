/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteSelect;

public class TestTwoSitePlans extends TestCase {

    static final String JAR = "distplanningregression.jar";

    ExecutionSite site1;
    ExecutionSite site2;
    ExecutionEngine ee1;
    ExecutionEngine ee2;

    Catalog catalog = null;
    Cluster cluster = null;
    Procedure selectProc = null;
    Statement selectStmt = null;
    PlanFragment selectTopFrag = null;
    PlanFragment selectBottomFrag = null;

    @Override
    public void setUp() throws IOException {
        VoltDB.instance().readBuildInfo("Test");

        // compile a catalog
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + JAR;

        TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(MultiSiteSelect.class, InsertNewOrder.class);

        pb.compile(catalogJar, 2, 0);


        // load a catalog
        byte[] bytes = CatalogUtil.toBytes(new File(catalogJar));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);

        // create the catalog (that will be passed to the ClientInterface
        catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // update the catalog with the data from the deployment file
        String pathToDeployment = pb.getPathToDeployment();
        assertTrue(CatalogUtil.compileDeploymentAndGetCRC(catalog, pathToDeployment, true) >= 0);

        cluster = catalog.getClusters().get("cluster");
        CatalogMap<Procedure> procedures = cluster.getDatabases().get("database").getProcedures();
        Procedure insertProc = procedures.get("InsertNewOrder");
        assert(insertProc != null);
        selectProc = procedures.get("MultiSiteSelect");
        assert(selectProc != null);

        // create two EEs
        site1 = new ExecutionSite(0); // site 0
        ee1 = new ExecutionEngineJNI(site1, cluster.getRelativeIndex(), 1, 0, 0, "", 100);
        ee1.loadCatalog( 0, catalog.serialize());
        site2 = new ExecutionSite(1); // site 1
        ee2 = new ExecutionEngineJNI(site2, cluster.getRelativeIndex(), 2, 0, 0, "", 100);
        ee2.loadCatalog( 0, catalog.serialize());

        // cache some plan fragments
        selectStmt = selectProc.getStatements().get("selectAll");
        assert(selectStmt != null);
        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i == 0) selectTopFrag = f;
            else selectBottomFrag = f;
            i++;
        }
        assert(selectTopFrag != null);
        assert(selectBottomFrag != null);

        if (selectTopFrag.getHasdependencies() == false) {
            PlanFragment temp = selectTopFrag;
            selectTopFrag = selectBottomFrag;
            selectBottomFrag = temp;
        }

        // insert some data
        Statement insertStmt = insertProc.getStatements().get("insert");
        assert(insertStmt != null);

        PlanFragment insertFrag = null;
        for (PlanFragment f : insertStmt.getFragments())
            insertFrag = f;
        ParameterSet params = new ParameterSet();
        params.m_params = new Object[] { 1L, 1L, 1L };

        VoltTable[] results = ee2.executeQueryPlanFragmentsAndGetResults(
                new long[] { CatalogUtil.getUniqueIdForFragment(insertFrag) }, 1,
                new ParameterSet[] { params }, 1,
                1,
                0,
                Long.MAX_VALUE);
        assert(results.length == 1);
        assert(results[0].asScalarLong() == 1L);

        params.m_params = new Object[] { 2L, 2L, 2L };

        results = ee1.executeQueryPlanFragmentsAndGetResults(
                new long[] { CatalogUtil.getUniqueIdForFragment(insertFrag) }, 1,
                new ParameterSet[] { params }, 1,
                2,
                1,
                Long.MAX_VALUE);
        assert(results.length == 1);
        assert(results[0].asScalarLong() == 1L);
    }

    public void testMultiSiteSelectAll() {
        ParameterSet params = new ParameterSet();
        params.m_params = new Object[] { };

        DependencyPair dependencies = ee1.executePlanFragment(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                1 | DtxnConstants.MULTIPARTITION_DEPENDENCY, -1,
                params, 3, 2, Long.MAX_VALUE);
        VoltTable dependency1 = dependencies.dependency;
        int depId1 = dependencies.depId;
        try {
            System.out.println(dependency1.toString());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertTrue(dependency1 != null);

        dependencies = ee2.executePlanFragment(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                1 | DtxnConstants.MULTIPARTITION_DEPENDENCY, -1,
                params, 3, 2, Long.MAX_VALUE);
        VoltTable dependency2 = dependencies.dependency;
        int depId2 = dependencies.depId;
        try {
            System.out.println(dependency2.toString());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertTrue(dependency2 != null);

        ee1.stashDependency(depId1, dependency1);
        ee1.stashDependency(depId2, dependency2);

        dependencies = ee1.executePlanFragment(
                CatalogUtil.getUniqueIdForFragment(selectTopFrag),
                2, 1 | DtxnConstants.MULTIPARTITION_DEPENDENCY,
                params, 3, 2, Long.MAX_VALUE);
        try {
            System.out.println("Final Result");
            System.out.println(dependencies.dependency.toString());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }



}
