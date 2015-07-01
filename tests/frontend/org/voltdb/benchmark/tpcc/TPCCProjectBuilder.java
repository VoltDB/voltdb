/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.benchmark.tpcc;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.voltdb.benchmark.tpcc.procedures.FragmentUpdateTestProcedure;
import org.voltdb.benchmark.tpcc.procedures.LoadWarehouse;
import org.voltdb.benchmark.tpcc.procedures.LoadWarehouseReplicated;
import org.voltdb.benchmark.tpcc.procedures.ResetWarehouse;
import org.voltdb.benchmark.tpcc.procedures.SelectAll;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerIdC;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerIdW;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerNameC;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerNameW;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;

/**
 * A subclass of VoltProjectBuilder that already knows about all of the
 * procedures, schema and partitioning info for TPC-C. It also contains
 * some helper code for other tests that use part of TPC-C.
 *
 */
public class TPCCProjectBuilder extends VoltProjectBuilder {

    /**
     * All procedures needed for TPC-C tests + benchmark
     */
    public static Class<?> PROCEDURES[] = new Class<?>[] {
        delivery.class, neworder.class, ostatByCustomerId.class,
        ostatByCustomerName.class, paymentByCustomerIdC.class,
        paymentByCustomerNameC.class, paymentByCustomerIdW.class,
        paymentByCustomerNameW.class, slev.class, SelectAll.class,
        ResetWarehouse.class, LoadWarehouse.class, FragmentUpdateTestProcedure.class,
        LoadWarehouseReplicated.class,
        paymentByCustomerName.class, paymentByCustomerId.class
    };

    public static String partitioning[][] = new String[][] {
        {"WAREHOUSE", "W_ID"},
        {"DISTRICT", "D_W_ID"},
        {"CUSTOMER", "C_W_ID"},
        {"HISTORY", "H_W_ID"},
        {"STOCK", "S_W_ID"},
        {"ORDERS", "O_W_ID"},
        {"NEW_ORDER", "NO_W_ID"},
        {"ORDER_LINE", "OL_W_ID"}
    };

    public static final URL ddlURL = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
    public static final String jarFilename = "tpcc.jar";
    private static final String m_jarFileName = "tpcc.jar";

    /**
     * Add the TPC-C procedures to the VoltProjectBuilder base class.
     */
    public void addDefaultProcedures() {
        addProcedures(PROCEDURES);
        addStmtProcedure("InsertCustomer", "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "CUSTOMER.C_W_ID: 2");
        addStmtProcedure("InsertWarehouse", "INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", "WAREHOUSE.W_ID: 0");
        addStmtProcedure("InsertStock", "INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "STOCK.S_W_ID: 1");
        addStmtProcedure("InsertOrders", "INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "ORDERS.O_W_ID: 2");
        addStmtProcedure("InsertOrderLine", "INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "ORDER_LINE.OL_W_ID: 2");
        addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");
        addStmtProcedure("InsertItem", "INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);");
        addStmtProcedure("InsertHistory", "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "HISTORY.H_W_ID: 4");
        addStmtProcedure("InsertDistrict", "INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "DISTRICT.D_W_ID: 1");
        addStmtProcedure("InsertCustomerName", "INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);");
    }

    /**
     * Add the TPC-C partitioning to the VoltProjectBuilder base class.
     */
    public void addDefaultPartitioning() {
        for (String pair[] : partitioning) {
            addPartitionInfo(pair[0], pair[1]);
        }
    }

    /**
     * Add the TPC-C schema to the VoltProjectBuilder base class.
     */
    public void addDefaultSchema() {
        addSchema(ddlURL);
    }

    public void addDefaultExport() {
        addExport(true /* enabled */);

        /* Fixed after the loader completes. */
        // setTableAsExportOnly("WAREHOUSE");
        // setTableAsExportOnly("DISTRICT");
        // setTableAsExportOnly("ITEM");
        // setTableAsExportOnly("CUSTOMER");
        // setTableAsExportOnly("CUSTOMER_NAME");
        // setTableAsExportOnly("STOCK");

        /* Modified by actual benchmark: approx 6.58 ins/del per txn. */
        // setTableAsExportOnly("HISTORY");       // 1 insert per payment (43%)
        // setTableAsExportOnly("ORDERS");        // 1 insert per new order (45%)
        // setTableAsExportOnly("NEW_ORDER");     // 1 insert per new order; 10 deletes per delivery (4%)
        // setTableAsExportOnly("ORDER_LINE");    // 10 inserts per new order */

        setTableAsExportOnly("HISTORY");
    }

    @Override
    public String[] compileAllCatalogs(int sitesPerHost,
                                       int length,
                                       int kFactor,
                                       String voltRoot) {
        addAllDefaults();
        Catalog catalog = compile(m_jarFileName, sitesPerHost,
                                  length, kFactor, voltRoot);
        if (catalog == null) {
            throw new RuntimeException("Bingo project builder failed app compilation.");
        }
        return new String[] {m_jarFileName};
    }

    @Override
    public void addAllDefaults() {
        addDefaultProcedures();
        addDefaultPartitioning();
        addDefaultSchema();
        // addDefaultExport();
    }

    public String getJARFilename() { return jarFilename; }

    /**
     * Get a pointer to a compiled catalog for TPCC with all the procedures.
     */
    public Catalog createTPCCSchemaCatalog() throws IOException {
        // compile a catalog
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "tpcc-jni.jar";

        addDefaultSchema();
        addDefaultPartitioning();
        addDefaultProcedures();

        Catalog catalog = compile(catalogJar, 1, 1, 0, null);
        assert(catalog != null);
        return catalog;
    }

    /**
     * Get a pointer to a compiled catalog for TPCC with all the procedures.
     * This can be run without worrying about setting up anything else in this class.
     */
    static public Catalog getTPCCSchemaCatalog() throws IOException {
        return (new TPCCProjectBuilder().createTPCCSchemaCatalog());
    }
}
