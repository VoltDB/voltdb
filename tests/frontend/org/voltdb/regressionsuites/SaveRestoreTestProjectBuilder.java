/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.saverestore.GetTxnId;
import org.voltdb_testprocs.regressionsuites.saverestore.MatView;
import org.voltdb_testprocs.regressionsuites.saverestore.SaveRestoreSelect;

public class SaveRestoreTestProjectBuilder extends VoltProjectBuilder
{
    public static Class<?> PROCEDURES[] =
        new Class<?>[] { MatView.class, SaveRestoreSelect.class, GetTxnId.class};
    public static Class<?> PROCEDURES_NOPARTITIONING[] =
            new Class<?>[] { MatView.class, SaveRestoreSelect.class};

    public static String partitioning[][] =
        new String[][] {{"PARTITION_TESTER", "PT_ID"},
                        {"CHANGE_COLUMNS", "ID"},
                        {"JUMBO_ROW", "PKEY"},
                        {"JUMBO_ROW_UTF8", "PKEY"},
                       };

    public static final URL ddlURL =
        MatView.class.getResource("saverestore-ddl.sql");
    public static final String jarFilename = "saverestore.jar";

    public SaveRestoreTestProjectBuilder addDefaultProcedures()
    {
        catBuilder().addProcedures(PROCEDURES);
        return this;
    }

    public SaveRestoreTestProjectBuilder addDefaultProceduresNoPartitioning()
    {
        catBuilder().addProcedures(PROCEDURES_NOPARTITIONING);
        return this;
    }

    public SaveRestoreTestProjectBuilder addDefaultPartitioning()
    {
        for (String pair[] : partitioning) {
            addPartitionInfo(pair[0], pair[1]);
        }
        return this;
    }

    public void addPartitioning(String tableName, String partitionColumnName)
    {
        addPartitionInfo(tableName, partitionColumnName);
    }

    public void addDefaultSchema()
    {
        catBuilder().addSchema(ddlURL);
    }

    // factory method
    static public SaveRestoreTestProjectBuilder defaultBuilder() {
        return new SaveRestoreTestProjectBuilder().addAllDefaults();
    }

    // factory method
    static public SaveRestoreTestProjectBuilder noPartitioningBuilder() {
        return new SaveRestoreTestProjectBuilder().addAllDefaultsNoPartitioning();
    }

    private SaveRestoreTestProjectBuilder addAllDefaults()
    {
        addDefaultSchema();
        addDefaultPartitioning();
        addDefaultProcedures();
        addStmtProcedure("JumboInsert", "INSERT INTO JUMBO_ROW VALUES ( ?, ?, ?)", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboSelect", "SELECT * FROM JUMBO_ROW WHERE PKEY = ?", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboCount", "SELECT COUNT(*) FROM JUMBO_ROW");
        addStmtProcedure("JumboInsertChars", "INSERT INTO JUMBO_ROW_UTF8 VALUES ( ?, ?, ?)", "JUMBO_ROW.PKEY: 0");
        addStmtProcedure("JumboSelectChars", "SELECT * FROM JUMBO_ROW_UTF8 WHERE PKEY = ?", "JUMBO_ROW.PKEY: 0");
        return this;
    }

    /*
     * Different default set for the test of ENG-696 TestReplicatedSaveRestoreSysprocSuite
     * Has no partitioned tables.
     */
    private SaveRestoreTestProjectBuilder addAllDefaultsNoPartitioning() {
        addDefaultProceduresNoPartitioning();
        addDefaultSchema();
        return this;
    }

    public String getJARFilename()
    {
        return jarFilename;
    }

}
