/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.planner.eegentests;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.planner.PlanSelector;
import org.voltdb.planner.PlannerTestCase;
import org.voltdb.plannodes.AbstractPlanNode;

/**
 * This class generates some C++ unit tests.
 */
public class EEPlanGenerator extends PlannerTestCase {

    private static final String TESTFILE_TEMPLATE =
        "\n" +
        "/******************************************************************************************\n" +
        " *\n" +
        " * NOTA BENE: This file is automagically generated from the source class named\n" +
        " *                @SOURCE_PACKAGE_NAME@.@SOURCE_CLASS_NAME@.\n" +
        " *            Please do not edit it unless you abandon all hope of regenerating it.\n" +
        " *\n" +
        " ******************************************************************************************/\n" +
        "#include \"harness.h\"\n" +
        "\n" +
        "#include \"catalog/cluster.h\"\n" +
        "#include \"catalog/table.h\"\n" +
        "#include \"plannodes/abstractplannode.h\"\n" +
        "#include \"storage/persistenttable.h\"\n" +
        "#include \"storage/temptable.h\"\n" +
        "#include \"storage/tableutil.h\"\n" +
        "#include \"test_utils/plan_testing_config.h\"\n" +
        "#include \"test_utils/LoadTableFrom.hpp\"\n" +
        "#include \"test_utils/plan_testing_baseclass.h\"\n" +
        "\n" +
        "\n" +
        "namespace {\n" +
        "extern TestConfig allTests[];\n" +
        "};\n" +
        "\n" +
        "class @TEST_CLASS_NAME@ : public PlanTestingBaseClass<EngineTestTopend> {\n" +
        "public:\n" +
        "    /*\n" +
        "     * This constructor lets us set the global random seed for the\n" +
        "     * random number generator.  It would be better to have a seed\n" +
        "     * just for this test.  But that is not easily done.\n" +
        "     */\n" +
        "    @TEST_CLASS_NAME@(uint32_t randomSeed = (unsigned int)time(NULL)) {\n" +
        "        initialize(m_testDB, randomSeed);\n" +
        "    }\n" +
        "\n" +
        "    ~@TEST_CLASS_NAME@() { }\n" +
        "protected:\n" +
        "    static DBConfig         m_testDB;\n" +
        "};\n" +
        "\n" +
        "/*\n" +
        " * All the test cases are here.\n" +
        " */\n" +
        "@TEST_CASES@\n" +
        "\n" +
        "namespace {\n" +
        "/*\n" +
        " * These are the names of all the columns.\n" +
        " */\n" +
        "@TABLE_COLUMN_NAMES@\n" +
        "\n" +
        "/*\n" +
        " * These are the types of all the columns.\n" +
        " */\n" +
        "@TABLE_TYPE_NAMES@\n" +
        "\n" +
        "/*\n" +
        " * These are the sizes of all the column data.\n" +
        " */\n" +
        "@TABLE_TYPE_SIZES@\n" +
        "\n" +
        "/*\n" +
        " * These are the strings in each populated columns.\n" +
        " * The data will either be integers or indices into this table.\n" +
        " */\n" +
        "@TABLE_STRINGS@\n" +
        "\n" +
        "/*\n" +
        " * This is the data in all columns.\n" +
        " */\n" +
        "@TABLE_DATA@\n" +
        "\n" +
        "/*\n" +
        " * These are the names of all the columns.\n" +
        " */\n" +
        "/*\n" +
        " * These knit together all the bits of data which form a table.\n" +
        " */\n" +
        "@TABLE_CONFIGS@\n" +
        "\n" +
        "/*\n" +
        " * This holds all the persistent tables.\n" +
        " */\n" +
        "@TABLE_DEFINITIONS@\n" +
        "\n" +
        "@ALL_TESTS@\n" +
        "}\n" +
        "\n" +
        "DBConfig @TEST_CLASS_NAME@::m_testDB =\n" +
        "\n" +
        "@DATABASE_CONFIG_BODY@\n" +
        "\n" +
        "int main() {\n" +
        "     return TestSuite::globalInstance()->runAll();\n" +
        "}\n";

    //
    // This holds the full path name of the directory into which we will
    // put generated EE unit tests.
    //
    private String m_testGenPath;
    //
    // This is the last path component of the previous.
    //
    private String m_testGenDir;
    //
    // This holds the full path name of the file which contains names
    // of generated tests.  The names are listed one test name per line.
    // Each test name is the full path name to the test file.
    //
    private String m_testNamesFile;

    protected String getPlanString(String sqlStmt, int fragmentNumber) throws JSONException {
        boolean planForSinglePartition = (fragmentNumber == 0);
        List<AbstractPlanNode> nodes = compileToFragments(sqlStmt, planForSinglePartition);
        if (nodes.size() <= fragmentNumber) {
            throw new PlanningErrorException(String.format("requested fragment number %d is out of range [0,%d)\n",
                                                           fragmentNumber,
                                                           nodes.size()));
        }
        String planString = PlanSelector.outputPlanDebugString(nodes.get(fragmentNumber));
        return planString;
    }

    /**
     * Pair q column name and a type. This is used for schemae.
     *
     * The length is the length of the datatype.  For non-strings
     * this is a property of the datatype.  For strings it must be
     * specified by the schema.
     */
    protected static class ColumnConfig {
        public ColumnConfig(Column col) {
            m_name = col.getName();
            m_type = VoltType.get((byte)col.getType());
            m_length = col.getSize();
        }
        public ColumnConfig(String name, VoltType type, int length) {
            assert(0 <= length || type != VoltType.STRING);
            m_name   = name;
            m_type   = type;
            m_length = length;
        }
        public ColumnConfig(String name, VoltType type) {
            this(name, type, type.getLengthInBytesForFixedTypes());
        }
        public final String getName() {
            return m_name;
        }
        public final VoltType getType() {
            return m_type;
        }
        public final int getLength() {
            return m_length;
        }
        public final String getColumnTypeName() {
            if (m_type == VoltType.STRING) {
                return "voltdb::ValueType::tVARCHAR";
            }
            return "voltdb::ValueType::t" + m_type.getName();
        }
        private String   m_name;
        private VoltType m_type;
        private int      m_length;
    }

    protected static class SchemaConfig {
        public SchemaConfig(ColumnConfig ...columns) {
            for (ColumnConfig cc : columns) {
                m_columns.add(cc);
            }
        }
        ColumnConfig getColumn(int idx) {
            return m_columns.get(idx);
        }
        public int getNumColumns() {
            return m_columns.size();
        }
        public List<ColumnConfig> getColumns() {
            return m_columns;
        }
        private List<ColumnConfig>  m_columns = new ArrayList<>();
    }

    /**
     * Define a schema from the catalog.
     */
    private static SchemaConfig makeSchemaConfig(String tableName,
                                                 Database db) {
        Table dbTable = db.getTables().get(tableName);
        assert(dbTable != null);
        ColumnConfig[] cols = new ColumnConfig[dbTable.getColumns().size()];
        for (Column col : dbTable.getColumns()) {
            cols[col.getIndex()] = new ColumnConfig(col);
        }
        return new SchemaConfig(cols);
    }

    /**
     * Fetch the definition of a Table from the catalog
     * in a format we can easily use.
     */
    protected static class TableConfig {
        /**
         * This constructor is for defining tables when
         * the data is fixed.
         *
         * @param tableName
         * @param schema
         * @param data
         */
        public TableConfig(String tableName,
                           Database db,
                           Object[][] data) {
            tableName = tableName.toUpperCase();
            SchemaConfig schema = makeSchemaConfig(tableName, db);
            m_tableName   = tableName;
            m_schema      = schema;
            if (data != null) {
                m_rowCount = data.length;
                m_data = computeData(data);
            } else {
                m_rowCount = 0;
                m_data = null;
            }
            ensureTable();
        }

        /**
         * This constructor is used for defining tables
         * when the data is randomly generated by the C++
         * unit test.  This randomly generated data is
         * generally useful for profiling large tables.
         *
         * @param tableName
         * @param schema
         * @param nrows
         */
        public TableConfig(String tableName,
                            Database db,
                           int nrows) {
            this(tableName, db, null);
            m_rowCount    = nrows;
        }

        public boolean hasActualData() {
            return m_data != null;
        }
        /*
         * Strings are stored in the string table.  The data
         * is either the integral data in the table, for integers,
         * or else an index into the string table for strings.
         */
        private int[][] computeData(Object[][] data) {
            int[][] answer = new int[data.length][m_schema.getNumColumns()];
            for (int ridx = 0; ridx < data.length; ridx += 1) {
                Object[] row = data[ridx];
                for (int cidx = 0; cidx < row.length; cidx += 1) {
                    Object obj = row[cidx];
                    if (obj instanceof Number) {
                        Number num = (Number)obj;
                        answer[ridx][cidx] = num.intValue();
                    } else if (obj instanceof String) {
                        answer[ridx][cidx] = addString((String)obj);
                    }
                }
            }
            return answer;
        }

        private int addString(String obj) {
            m_strings.add(obj);
            return m_strings.size()-1;
        }
        public List<String> getStrings() {
            return m_strings;
        }
        public String getTableRowCountName() {
            return String.format("NUM_TABLE_ROWS_%s", m_tableName.toUpperCase());
        }
        public String getTableColCountName() {
            return String.format("NUM_TABLE_COLS_%s", m_tableName.toUpperCase());
        }
        public int getRowCount() {
            return m_rowCount;
        }
        public List<ColumnConfig> getColumns() {
            return m_schema.getColumns();
        }
        public String getColumnNamesName() {
            return String.format("%s_ColumnNames", m_tableName);
        }
        public String getColumnTypesName() {
            return String.format("%s_Types", m_tableName);
        }
        public String getColumnTypesSizesName() {
            return String.format("%s_Sizes", m_tableName);
        }
        public String getTableConfigName() {
            return String.format("%sConfig", m_tableName);
        }

        public final SchemaConfig getSchema() {
            return m_schema;
        }
        private void ensureTable() {
            // If there is not actual data, all is well.
            if (m_data == null) {
                return;
            }
            // Ensure there is at least one row, and that
            // all rows have the same length, and that the
            // types are all sensible.
            assert(m_data.length > 0);
            for (int idx = 1; idx < m_data.length; idx += 1) {
                assert(m_data[idx].length == m_data[0].length);
            }
        }

        public String getTableDataName() {
            if (hasActualData()) {
                return String.format("%sData", m_tableName);
            } else {
                return "NULL";
            }
        }

        public String  getStringTableName() {
            return String.format("%s_Strings", m_tableName);
        }

        public String getStringsName() {
            return "NULL";
        }
        public Object getNumStringsName() {
            return "num_" + m_tableName + "_strings";
        }

        public Object getNumStrings() {
            return m_strings.size();
        }
        String m_tableName;
        SchemaConfig m_schema;
        int[][] m_data = null;
        List<String> m_strings = new ArrayList<>();
        int m_rowCount;
    }

    protected Database getDatabase() {
        Database db = getCatalog().getClusters().get("cluster").getDatabases().get("database");
        return db;
    }

    /**
     * Define a database.
     */
    protected class DBConfig {
        public DBConfig(Class<? extends PlannerTestCase>    klass,
                 URL                                 ddlURL,
                 String                              catalogString,
                 TableConfig ...                     tables) {
            m_class  = klass;
            m_ddlURL = ddlURL;
            m_catalogString = catalogString;
            m_tables = Arrays.asList(tables);
            m_testConfigs = new ArrayList<>();
        }

        /**
         * Clean up a string used to write a C++ string.  Escape double
         * quotes and newlines.
         *
         * @param input
         * @return
         */
        private String cleanString(String input, String indent) {
            String quotedInput = input.replace("\"", "\\\"");
            quotedInput = "\"" + quotedInput.replace("\n", "\\n\"\n" + indent + "\"") + "\"";
            return quotedInput;
        }

        /**
         * Given a URL, from Class.getResource(), pull in the contents of the DDL file.
         *
         * @param ddlURL
         * @return
         * @throws Exception
         */
        private String getDDLStringFromURL() throws Exception {
            InputStream inputStream = null;
            try {
                inputStream = m_ddlURL.openStream();
                ByteArrayOutputStream result = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = inputStream.read(buffer)) != -1) {
                    result.write(buffer, 0, length);
                }
                return result.toString("UTF-8");
            } finally {
                try {
                    if (inputStream != null) {
                        inputStream.close();
                    }
                } catch (Exception ex) {
                    ;
                }
            }
        }

        /**
         * Add a test configuration to the database.
         *
         * @param testConfig
         */
        public void addTest(TestConfig testConfig) {
            m_testConfigs.add(testConfig);
        }

        private URL                                   m_ddlURL;
        private String                                m_catalogString;
        private List<TableConfig>                     m_tables;
        private List<TestConfig>                      m_testConfigs;
        private Class<? extends PlannerTestCase>      m_class;

        public String getTestCases(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (int testIdx = 0; testIdx < m_testConfigs.size(); testIdx += 1) {
                TestConfig tc = m_testConfigs.get(testIdx);
                sb.append(String.format("TEST_F(%s, %s) {\n" +
                                        "    static int testIndex = %d;\n" +
                                        "    executeTest(allTests[testIndex]);\n" +
                                        "}\n",
                                        params.get("TEST_CLASS_NAME"),
                                        tc.m_testName,
                                        testIdx));
            }
            return sb.toString();
        }

        public String getTableColumnTypesString(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (TableConfig tc : m_tables) {
                getOneTableColumnTypesString(sb, tc);
            }
            return sb.toString();
        }

        private void getOneTableColumnTypesString(StringBuffer sb, TableConfig tc) {
            sb.append(String.format("const voltdb::ValueType %s[] = {\n", tc.getColumnTypesName()));
            for (int idx = 0; idx < tc.getColumns().size(); idx += 1) {
                ColumnConfig cc = tc.getColumns().get(idx);
                sb.append("    " + cc.getColumnTypeName() + ",\n");
            }
            sb.append("};\n");
        }

        public String getTableColumnSizeString(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (TableConfig tc : m_tables) {
                getOneTableColumnSizeString(sb, tc);
            }
            return sb.toString();
        }

        private void getOneTableColumnSizeString(StringBuffer sb, TableConfig tc) {
            sb.append(String.format("const int32_t %s[] = {\n", tc.getColumnTypesSizesName()));
            for (int idx = 0; idx < tc.getColumns().size(); idx += 1) {
                ColumnConfig cc = tc.getColumns().get(idx);
                sb.append("    " + cc.getLength() + ",\n");
            }
            sb.append("};\n");
        }

        public String getTableColumnNames(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (TableConfig tc : m_tables) {
                getOneTableColumnNames(sb, tc);
            }
            return sb.toString();
        }

        private void getOneTableColumnNames(StringBuffer sb, TableConfig tc) {
            sb.append(String.format("const char *%s[] = {\n", tc.getColumnNamesName()));
            for (int idx = 0; idx < tc.getColumns().size(); idx += 1) {
                ColumnConfig cc = tc.getColumns().get(idx);
                String colName = cc.getName();
                sb.append("    \"" + colName + "\",\n");
            }
            sb.append("};\n");
        }

        private void writeTable(StringBuffer sb,
                                String tableName,
                                String rowCountName,
                                int rowCount,
                                String colCountName,
                                int colCount,
                                int[][] data) {
            sb.append(String.format("const int %s = %d;\n", rowCountName, rowCount));
            sb.append(String.format("const int %s = %d;\n", colCountName, colCount));
            //
            // If there is no data, don't declare it.
            // We'll fill the table later on.
            //
            if (data == null) {
                sb.append(";\n");
            } else {
                sb.append(String.format("const int %s[%s * %s] = {\n",
                                        tableName,
                                        rowCountName,
                                        colCountName));
                for (int ridx = 0; ridx < data.length; ridx += 1) {
                    sb.append("    ");
                    for (int cidx = 0; cidx < data[ridx].length; cidx += 1) {
                        sb.append(String.format("%3d,", data[ridx][cidx]));
                    }
                    sb.append("\n");
                }
                sb.append("};\n\n");
            }
        }

        public String getTableStrings(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (TableConfig tc : m_tables) {
                getOneTableStrings(sb, tc);
            }
            return sb.toString();
        }

        private void getOneTableStrings(StringBuffer sb, TableConfig tc) {
            sb.append(String.format("int32_t %s = %d;\n", tc.getNumStringsName(), tc.getNumStrings()));
            sb.append(String.format("const char *%s[] = {\n",
                                    tc.getStringTableName()));
            for (String str : tc.getStrings()) {
                sb.append(String.format("    \"%s\",\n", str));
            }
            sb.append("};\n");
        }

        public String getAllTableData(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (TableConfig tc : m_tables) {
                getOneTableData(sb, tc);
            }
            return sb.toString();
        }

        private void getOneTableData(StringBuffer sb, TableConfig tc) {
            String rowCountName = tc.getTableRowCountName();
            String colCountName = tc.getTableColCountName();
            writeTable(sb,
                       tc.getTableDataName(),
                       rowCountName,
                       tc.getRowCount(),
                       colCountName,
                       tc.getColumns().size(),
                       tc.m_data);
        }

        public String getTableConfigs(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            for (TableConfig tc : m_tables) {
                writeOneTableConfig(sb, tc);
            }
            return sb.toString();
        }

        private void writeOneTableConfig(StringBuffer sb, TableConfig tc) {
            sb.append(String.format("const TableConfig %s = {\n", tc.getTableConfigName()))
              .append(String.format("    \"%s\",\n", tc.m_tableName))
              .append(String.format("    %s,\n", tc.getColumnNamesName()))
              .append(String.format("    %s,\n", tc.getColumnTypesName()))
              .append(String.format("    %s,\n", tc.getColumnTypesSizesName()))
              .append(String.format("    %s,\n", tc.getTableRowCountName()))
              .append(String.format("    %s,\n", tc.getTableColCountName()))
              .append(String.format("    %s,\n", tc.getTableDataName()))
              .append(String.format("    %s,\n", tc.getStringTableName()))
              .append(String.format("    %s\n",  tc.getNumStringsName()))
              .append("};\n");
        }

        public String getTableDefinitions(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            sb.append("const TableConfig *allTables[] = {\n");
            for (TableConfig tc : m_tables) {
                sb.append(String.format("    &%s,\n", tc.getTableConfigName()));
            }
            sb.append("};\n");
            return sb.toString();
        }

        public String getTestResultData(Map<String, String> params) {
            StringBuffer sb = new StringBuffer();
            sb.append("const TableConfig *allResults[] = {\n");
            for (TestConfig tstConfig : m_testConfigs) {
                if (tstConfig.hasExpectedData()) {
                    sb.append(String.format("    &%s,\n",
                                            tstConfig.getExpectedOutput().getTableConfigName()));
                }
            }
            sb.append("\n};\n");
            return sb.toString();
        }

        public String getAllTests(Map<String, String> params) throws JSONException {
            StringBuffer sb = new StringBuffer();
            sb.append(String.format("TestConfig allTests[%d] = {\n", m_testConfigs.size()));
            for (TestConfig tc : m_testConfigs) {
                sb.append("    {\n")
                  .append("        // SQL Statement\n")
                  .append(String.format("        %s,\n", cleanString(tc.m_sqlString, "        ")))
                  .append("        // Failure is expected\n")
                  .append(String.format("        %s,\n", tc.isExpectedToFail() ? "true" : "false"))
                  .append("        // Plan String\n")
                  .append(String.format("        %s,\n",
                                        cleanString(getPlanString(tc.m_sqlString, tc.getPlanFragment()), "        ")))
                  .append(String.format("        %s\n",  tc.getOutputTableName()))
                  .append("    },\n");
            }
            sb.append("};\n");
            return sb.toString();
        }

        public String getDatabaseConfigBody(Map<String, String> params) throws Exception {
            StringBuffer sb = new StringBuffer();
            sb.append("{\n");
            sb.append("    //\n    // DDL.\n    //\n");
            sb.append(String.format("    %s,\n", cleanString(getDDLStringFromURL(), "    ")));
            sb.append("    //\n    // Catalog String\n    //\n");
            sb.append(String.format("    %s,\n", cleanString(m_catalogString, "    ")));
            sb.append(String.format("    %d,\n", m_tables.size()));
            sb.append("    allTables\n");
            sb.append("};\n");
            return sb.toString();
        }

        public String getClassPackageName() {
            return m_class.getPackage().getName();
        }

        public String getClassName() {
            return m_class.getSimpleName();
        }
    }

    /**
     * Define a test.  We need the sql string and the expected tabular output.
     * @author bwhite
     */
    protected static class TestConfig {
        TestConfig(String       testName,
                   String       sqlString,
                   boolean      expectFail,
                   TableConfig  expectedOutput) {
            m_testName       = testName;
            m_sqlString      = sqlString;
            m_expectedOutput = expectedOutput;
            m_expectFail     = expectFail;
            m_planFragment   = 0;
        }

        public TestConfig(String testName,
                   String sqlString,
                   boolean expectFail) {
            this(testName, sqlString, expectFail, null);
        }

        /**
         * Tell whether this test has expected data.
         * @return
         */
        public boolean hasExpectedData() {
            return m_expectedOutput != null;
        }

        /**
         * Return the expected output table.  This may be null.
         * @return The expected output table, or null if the output is unspecified.
         */
        public TableConfig getExpectedOutput() {
            return m_expectedOutput;
        }

        /**
         * Return true if this test is expected to fail.
         * @return true iff this test is expected to fail.
         */
        public boolean isExpectedToFail() {
            return m_expectFail;
        }
        /**
         * Return the number of rows in the expected
         * output.  If this is -1, there is no expected
         * output.
         * @return
         */
        public int    getRowCount() {
            if (hasExpectedData()) {
                return m_expectedOutput.getRowCount();
            }
            return -1;
        }

        /**
         * Return the number of columns in the expected
         * output.  If this is -1 there is no expected
         * output.
         * @return
         */
        public int    getColCount() {
            if (hasExpectedData()) {
                return m_expectedOutput.getColumns().size();
            }
            return -1;
        }

        public String getColCountName() {
            return String.format("NUM_OUTPUT_COLS_%s", m_testName.toUpperCase());
        }

        public String getRowCountName() {
            return String.format("NUM_OUTPUT_ROWS_%s", m_testName.toUpperCase());
        }

        public String getOutputTableName() {
            if (hasExpectedData()) {
                return "&" + m_expectedOutput.getTableConfigName();
            } else {
                return "NULL";
            }
        }

        public TestConfig setPlanFragment(int number) {
            m_planFragment = number;
            return this;
        }

        public int getPlanFragment() {
            return m_planFragment;
        }
        private String      m_testName;
        private String      m_sqlString;
        private TableConfig m_expectedOutput;
        private boolean     m_expectFail;
        private int         m_planFragment;
    }

    /**
     * Given a foldername and a class name, write a C++ file for the test.
     *
     * @param string
     * @param string2
     * @param db
     * @throws Exception
     */
    protected void generateTests(String testFolder, String testClassName, DBConfig db) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("SOURCE_PACKAGE_NAME",   db.getClassPackageName());
        params.put("SOURCE_CLASS_NAME",     db.getClassName());
        params.put("TEST_CLASS_NAME",       testClassName);
        params.put("TEST_CASES",            db.getTestCases(params));
        params.put("TABLE_COLUMN_NAMES",    db.getTableColumnNames(params));
        params.put("TABLE_TYPE_SIZES",      db.getTableColumnSizeString(params));
        params.put("TABLE_STRINGS",         db.getTableStrings(params));
        params.put("TABLE_TYPE_NAMES",      db.getTableColumnTypesString(params));
        params.put("TABLE_DATA",            db.getAllTableData(params));
        params.put("TABLE_CONFIGS",         db.getTableConfigs(params));
        params.put("TABLE_DEFINITIONS",     db.getTableDefinitions(params));
        params.put("TEST_RESULT_TABLE_DEFINITIONS",
                                            db.getTestResultData(params));
        params.put("ALL_TESTS",             db.getAllTests(params));
        params.put("DATABASE_CONFIG_BODY",  db.getDatabaseConfigBody(params));
        writeTestFile(testFolder, testClassName, params);
        writeTestFileName(String.format("%s/%s/%s\n", m_testGenDir, testFolder, testClassName), true);
    }

    public static boolean typeMatch(Object elem, VoltType type, int size) {
        switch (type) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
            return elem instanceof Number && ! (elem instanceof Float || elem instanceof Double);
        case FLOAT:
            // We can't pass floats yet.  Sorry.
            return false;
        case STRING:
            if (! (elem instanceof String) ) {
                return false;
            }
            String elemStr = (String)elem;
            if (0 <= size && elemStr.length() > size) {
                return false;
            }
            return true;
        default:
            return false;
        }
    }

    private String readFile(File path) throws IOException {
        return(new String(Files.readAllBytes(Paths.get(path.getAbsolutePath()))));
    }

    /*
     * Only write the file if it is different from the
     * existing contents.  This saves building sometimes.
     */
    private void writeFile(File path, String contents) throws Exception {
        PrintWriter out = null;
        String oldContents = null;
        try {
            oldContents = readFile(path);
        } catch (IOException ex) {
            oldContents = "";
        }
        if (!oldContents.equals(contents)) {
            try {
                out = new PrintWriter(path);
                out.print(contents);
            } finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (Exception ex) {
                        ;
                    }
                }
            }
        }
    }

    protected void processArgs(String args[]) throws PlanningErrorException {
        for (int idx = 0; idx < args.length; idx += 1) {
            String arg = args[idx];
            if (arg.startsWith("--generated-source-dir=")) {
                m_testGenPath = arg.substring("--generated-source-dir=".length());
                // Pull out the last component.  We
                // need this later on, to put the source in the
                // right directory.
                String[] paths = m_testGenPath.split("/");
                if (paths.length == 0) {
                    throw new PlanningErrorException(
                            String.format("--generated-source-dir argument \"%s\" is malformed.",
                                          arg));
                }
                m_testGenDir = paths[paths.length - 1];
            } else if (arg.startsWith("--test-names-file=")) {
                m_testNamesFile = arg.substring("--test-names-file=".length());
            } else {
                throw new PlanningErrorException("Unknown generated sources argument: " + arg);
            }
        }
        if (m_testGenPath == null) {
            throw new PlanningErrorException("--generated-source-dir argument is missing in call to EEPlanGenerator.java");
        }
        if (m_testNamesFile == null) {
            throw new PlanningErrorException("--test-names-file argument is missing in call to EEPlanGenerator.java");
        }
    }

    /**
     * Write the named string to the output file.
     *
     * @param name The string to write.
     * @param append If true, then append to the file.  Otherwise truncate the file first.
     * @throws FileNotFoundException
     */
    protected void writeTestFileName(String name,
                                     boolean append) throws FileNotFoundException {
        File outFileName = new File(m_testNamesFile);
        if ( ! outFileName.exists() ) {
            append = false;
        }
        try (PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(outFileName, append)))) {
            ps.print(name);
        }
    }

    private void writeTestFile(String testFolder, String testClassName, Map<String, String> params) throws Exception {
        String template = TESTFILE_TEMPLATE;
        // This could be made much faster by looking for all the strings
        // with a regular expression, rather than one at a time.
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String pattern = "@" + entry.getKey() + "@";
            String value   = params.get(entry.getKey());
            template = template.replace(pattern, value);
        }
        if (template.isEmpty()) {
            throw new PlanningErrorException("Cannot create C++ Unit Test source from template.  This is a bug.");
        }
        File outputDir = new File(String.format("%s/%s", m_testGenPath, testFolder));
        if (! outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Cannot make test source folder \"" + outputDir + "\"");
        }
        File outputFile = new File(outputDir, testClassName + ".cpp");
        writeFile(outputFile, template);
    }

    public void setUp(URL ddlURL, String basename, boolean planForSinglePartition) throws Exception {
        setupSchema(ddlURL, basename, planForSinglePartition);
    }
}
