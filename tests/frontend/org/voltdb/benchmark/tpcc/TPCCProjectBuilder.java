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
import org.voltdb.catalog.Database;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

/**
 * A class that sets up the procedures, schema and partitioning info for TPC-C.
 * It also contains some helper code for other tests that use part of TPC-C.
 */
public class TPCCProjectBuilder {

    /**
     * All procedures needed for TPC-C tests + benchmark
     */
    public static Class<?> PROCEDURES[] = {
        delivery.class, neworder.class, ostatByCustomerId.class,
        ostatByCustomerName.class, paymentByCustomerIdC.class,
        paymentByCustomerNameC.class, paymentByCustomerIdW.class,
        paymentByCustomerNameW.class, slev.class, SelectAll.class,
        ResetWarehouse.class, LoadWarehouse.class, FragmentUpdateTestProcedure.class,
        LoadWarehouseReplicated.class,
        paymentByCustomerName.class, paymentByCustomerId.class
    };

    public static String partitioning[][] = {
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

    /**
     * Add the TPC-C partitioning to the CatalogBuilder.
     */
    public static CatalogBuilder addDefaultPartitioning(CatalogBuilder cb) {
        for (String pair[] : partitioning) {
            cb.addPartitionInfo(pair[0], pair[1]);
        }
        return cb;
    }

   // factory method
    public static CatalogBuilder defaultCatalogBuilder() {
        return addDefaultPartitioning(new CatalogBuilder())
        .addSchema(ddlURL)
        .addProcedures(PROCEDURES)
        .addStmtProcedure("InsertCustomer", "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "CUSTOMER.C_W_ID", 2)
        .addStmtProcedure("InsertWarehouse", "INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", "WAREHOUSE.W_ID", 0)
        .addStmtProcedure("InsertStock", "INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "STOCK.S_W_ID", 1)
        .addStmtProcedure("InsertOrders", "INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "ORDERS.O_W_ID", 2)
        .addStmtProcedure("InsertOrderLine", "INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "ORDER_LINE.OL_W_ID", 2)
        .addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID", 2)
        .addStmtProcedure("InsertItem", "INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);")
        .addStmtProcedure("InsertHistory", "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "HISTORY.H_W_ID", 4)
        .addStmtProcedure("InsertDistrict", "INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "DISTRICT.D_W_ID", 1)
        .addStmtProcedure("InsertCustomerName", "INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);")
        ;
        // addDefaultExport();
    }

    // factory method
    public static CatalogBuilder catalogBuilderNoProcs() {
        return addDefaultPartitioning(new CatalogBuilder()).addSchema(ddlURL);
        // addDefaultExport();
    }

    /**
     * Get a pointer to a compiled catalog for TPCC with all the procedures.
     */
    public static Catalog createTPCCSchemaCatalog() throws IOException {
        // compile a catalog
        CatalogBuilder cb = defaultCatalogBuilder();
        File catalogJar = File.createTempFile("TPCCProjectBuilder", ".jar");
        String catalogPath = catalogJar.getAbsolutePath();
        if ( ! cb.compile(catalogPath)) {
            return null;
        }
        byte[] bytes = MiscUtils.fileToBytes(catalogJar);
        Catalog catalog = CatalogUtil.deserializeCatalogFromJarFileBytes(bytes);
        assert(catalog != null);
        return catalog;
    }

    /**
     * Get the database from a compiled catalog for TPCC with all the procedures.
     */
    static public Database createTPCCSchemaDatabase() throws IOException {
        return createTPCCSchemaCatalog()
                .getClusters().get("cluster")
                .getDatabases().get("database");
    }

    public static Database createTPCCSchemaOriginalDatabase() {
        // TODO: Compile a database without the jar serialization/deserialization steps that
        // lose the annotations. That's the only way the calling test will succeed.
        // As things stand that takes a bit of refactoring because all of these things are
        // unfortunately tightly bound.
        try {
            return createTPCCSchemaCatalog()
                    .getClusters().get("cluster")
                    .getDatabases().get("database");
        }
        catch (IOException io) {
            return null;
        }
    }
}
