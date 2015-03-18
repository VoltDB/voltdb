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

package org.voltdb.regressionsuites;

import java.net.URL;

import org.voltdb.compiler.CatalogBuilder;
import org.voltdb_testprocs.regressionsuites.saverestore.GetTxnId;
import org.voltdb_testprocs.regressionsuites.saverestore.MatView;
import org.voltdb_testprocs.regressionsuites.saverestore.SaveRestoreSelect;

public class SaveRestoreTestProjectBuilder
{
    private static String partitioning[][] = {
        {"PARTITION_TESTER", "PT_ID"},
        {"CHANGE_COLUMNS", "ID"},
        {"JUMBO_ROW", "PKEY"},
        {"JUMBO_ROW_UTF8", "PKEY"},
        };

    private static final URL ddlURL = MatView.class.getResource("saverestore-ddl.sql");

    public static CatalogBuilder defaultProceduresNoPartitioning(URL url)
    {
        return new CatalogBuilder()
        .addSchema(url)
        .addProcedures(MatView.class, SaveRestoreSelect.class);
    }

    private static CatalogBuilder addDefaultPartitioningTo(CatalogBuilder cb)
    {
        for (String pair[] : partitioning) {
            cb.addPartitionInfo(pair[0], pair[1]);
        }
        return cb;
    }

    // factory method
    static public CatalogBuilder defaultBuilder() {
        return addDefaultPartitioningTo(
                new CatalogBuilder()
                .addProcedures(MatView.class, SaveRestoreSelect.class, GetTxnId.class)
                .addSchema(ddlURL)
                .addStmtProcedure("JumboInsert", "INSERT INTO JUMBO_ROW VALUES ( ?, ?, ?)",
                        "JUMBO_ROW.PKEY", 0)
                .addStmtProcedure("JumboSelect", "SELECT * FROM JUMBO_ROW WHERE PKEY = ?",
                        "JUMBO_ROW.PKEY", 0)
                .addStmtProcedure("JumboCount", "SELECT COUNT(*) FROM JUMBO_ROW")
                .addStmtProcedure("JumboInsertChars", "INSERT INTO JUMBO_ROW_UTF8 VALUES ( ?, ?, ?)",
                        "JUMBO_ROW.PKEY", 0)
                .addStmtProcedure("JumboSelectChars", "SELECT * FROM JUMBO_ROW_UTF8 WHERE PKEY = ?",
                        "JUMBO_ROW.PKEY", 0)
                );
    }

    // factory method
    /*
     * Different default set for the test of ENG-696 TestReplicatedSaveRestoreSysprocSuite
     * Has no partitioned tables.
     */
    static public CatalogBuilder noPartitioningBuilder() {
        return new CatalogBuilder()
        .addProcedures(MatView.class, SaveRestoreSelect.class)
        .addSchema(ddlURL)
        ;
    }

    public static CatalogBuilder alternativeCatalogBuilder()
    {
        return alternativeCatalogBuilder(
                SaveRestoreTestProjectBuilder.class.getResource("saverestore-altered-ddl.sql"));
    }

    public static CatalogBuilder alternativeCatalogBuilder(URL altSchema)
    {
        return addDefaultPartitioningTo(new CatalogBuilder()
                .addProcedures(MatView.class, SaveRestoreSelect.class, GetTxnId.class)
                .addSchema(altSchema));
    }

}
