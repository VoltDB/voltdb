/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.planner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

import junit.framework.TestCase;

public class PlannerTestCase extends TestCase {

    private PlannerTestAideDeCamp m_aide;
    private boolean m_byDefaultInferPartitioning = true;
    private boolean m_byDefaultPlanForSinglePartition;
    final private int m_defaultParamCount = 0;
    private String m_noJoinOrder = null;

    /**
     * @param sql
     * @return
     */
    private int countQuestionMarks(String sql) {
        int paramCount = 0;
        int skip = 0;
        while (true) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            skip = sql.indexOf('?', skip);
            if (skip == -1) {
                break;
            }
            skip++;
            paramCount++;
        }
        return paramCount;
    }

    protected void failToCompile(String sql, String... patterns)
    {
        int paramCount = countQuestionMarks(sql);
        try {
            List<AbstractPlanNode> unexpected = m_aide.compile(sql, paramCount,
                    m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, null);
            printExplainPlan(unexpected);
            fail("Expected planner failure, but found success.");
        }
        catch (Exception ex) {
            String result = ex.toString();
            for (String pattern : patterns) {
                if ( ! result.contains(pattern)) {
                    fail("Did not find pattern '" + pattern + "' in error string '" + result + "'");
                }
            }
        }
    }

    protected CompiledPlan compileAdHocPlan(String sql) {
        return compileAdHocPlan(sql, DeterminismMode.SAFER);
    }

    protected CompiledPlan compileAdHocPlan(String sql, DeterminismMode detMode) {
        CompiledPlan cp = null;
        try {
            cp = m_aide.compileAdHocPlan(sql, detMode);
            assertTrue(cp != null);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
        return cp;
    }

    /**
     * Fetch compiled planned based on provided partitioning information.
     * @param sql: SQL statement
     * @param inferPartitioning: Flag to indicate whether to use infer or forced partitioning
     *                           when generating plan. True to use infer partitioning info,
     *                           false for forced partitioning
     * @param forcedSP: Flag to indicate whether to generate plan for forced SP or MP.
     *                  If inferPartitioing flag is set to true, this flag is ignored
     * @param detMode: Specifies determinism mode - Faster or Safer
     * @return: Compiled plan based on specified input parameters
     */

    protected CompiledPlan compileAdHocPlan(String sql,
                                            boolean inferPartitioning,
                                            boolean forcedSP,
                                            DeterminismMode detMode) {
        CompiledPlan cp = null;
        try {
            cp = m_aide.compileAdHocPlan(sql, inferPartitioning, forcedSP, detMode);
            assertTrue(cp != null);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        return cp;
    }

    protected CompiledPlan compileAdHocPlan(String sql,
                                            boolean inferPartitioning,
                                            boolean forcedSP) {
        return compileAdHocPlan(sql, inferPartitioning, forcedSP, DeterminismMode.SAFER);
    }

    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileToFragments(String sql) {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, m_noJoinOrder);
    }

    protected List<AbstractPlanNode> compileToFragmentsForSinglePartition(String sql) {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, m_noJoinOrder);
    }


    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, String joinOrder) {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder) {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileWithJoinOrderToFragments(sql, paramCount, planForSinglePartition, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, int paramCount,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder) {
        List<AbstractPlanNode> pn = m_aide.compile(sql, paramCount, m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, joinOrder);
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        if (planForSinglePartition) {
            assertTrue(pn.size() == 1);
        }
        return pn;
    }

    protected AbstractPlanNode compileSPWithJoinOrder(String sql, String joinOrder) {
        try {
            return compileWithCountedParamsAndJoinOrder(sql, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
            return null;
        }
    }

    protected void compileWithInvalidJoinOrder(String sql, String joinOrder) throws Exception {
        compileWithJoinOrderToFragments(sql, m_defaultParamCount, m_byDefaultPlanForSinglePartition, joinOrder);
    }


    private AbstractPlanNode compileWithCountedParamsAndJoinOrder(String sql,
                                                                  String joinOrder) throws Exception {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileSPWithJoinOrder(sql, paramCount, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compile(String sql) {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileSPWithJoinOrder(sql, paramCount, null);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileForSinglePartition(String sql) {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        boolean m_infer = m_byDefaultInferPartitioning;
        boolean m_forceSP = m_byDefaultInferPartitioning;
        m_byDefaultInferPartitioning = false;
        m_byDefaultPlanForSinglePartition = true;

        AbstractPlanNode pn = compileSPWithJoinOrder(sql, paramCount, null);
        m_byDefaultInferPartitioning = m_infer;
        m_byDefaultPlanForSinglePartition = m_forceSP;
        return pn;
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileSPWithJoinOrder(String sql,
                                                      int paramCount,
                                                      String joinOrder) {
        List<AbstractPlanNode> pns = null;
        try {
            pns = compileWithJoinOrderToFragments(sql, paramCount, m_byDefaultPlanForSinglePartition, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
        assertTrue(pns.get(0) != null);
        return pns.get(0);
    }

    /**
     *  Find all the aggregate nodes in a fragment, whether they are hash, serial or partial.
     * @param fragment     Fragment to search for aggregate plan nodes
     * @return a list of all the nodes we found
     */
    protected static List<AbstractPlanNode> findAllAggPlanNodes(AbstractPlanNode fragment) {
        List<AbstractPlanNode> aggNodes = fragment.findAllNodesOfType(PlanNodeType.AGGREGATE);
        List<AbstractPlanNode> hashAggNodes = fragment.findAllNodesOfType(PlanNodeType.HASHAGGREGATE);
        List<AbstractPlanNode> partialAggNodes = fragment.findAllNodesOfType(PlanNodeType.PARTIALAGGREGATE);

        aggNodes.addAll(hashAggNodes);
        aggNodes.addAll(partialAggNodes);
        return aggNodes;
    }


    protected void setupSchema(URL ddlURL, String basename,
                               boolean planForSinglePartition) throws Exception {
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
        m_byDefaultPlanForSinglePartition = planForSinglePartition;
    }

    protected void setupSchema(boolean inferPartitioning, URL ddlURL,
                               String basename) throws Exception {
        m_byDefaultInferPartitioning = inferPartitioning;
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
    }

    public String getCatalogString() {
        return m_aide.getCatalogString();
    }

    Database getDatabase() {
        return m_aide.getDatabase();
    }

    protected void printExplainPlan(List<AbstractPlanNode> planNodes) {
        for (AbstractPlanNode apn: planNodes) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    protected String buildExplainPlan(List<AbstractPlanNode> planNodes) {
        String explain = "";
        for (AbstractPlanNode apn: planNodes) {
            explain += apn.toExplainPlanString() + '\n';
        }
        return explain;
    }

    protected void checkQueriesPlansAreTheSame(String sql1, String sql2) {
        String explainStr1, explainStr2;
        List<AbstractPlanNode> pns = compileToFragments(sql1);
        explainStr1 = buildExplainPlan(pns);
        pns = compileToFragments(sql2);
        explainStr2 = buildExplainPlan(pns);

        assertEquals(explainStr1, explainStr2);
    }

    /**
     * Call this function to verify that an order by plan node has the
     * sort expressions and directions we expect.
     *
     * @param orderByPlanNode The plan node to test.
     * @param columnDescrs Pairs of expressions and sort directions. There
     *                     must be an even number of these, the even
     *                     numbered ones must be expressions and the odd
     *                     numbered ones must be sort directions.  This is
     *                     numbering starting at 0.  So, they must be in
     *                     the order expr, direction, expr, direction, and
     *                     so forth.
     */
    protected void verifyOrderByPlanNode(OrderByPlanNode  orderByPlanNode,
                                         Object       ... columnDescrs) {
        // We should have an even number of columns
        assertEquals(0, columnDescrs.length % 2);
        List<AbstractExpression> exprs = orderByPlanNode.getSortExpressions();
        List<SortDirectionType>  dirs  = orderByPlanNode.getSortDirections();
        assertEquals(exprs.size(), dirs.size());
        assertEquals(columnDescrs.length/2, exprs.size());
        for (int idx = 0; idx < exprs.size(); idx += 1) {
            AbstractExpression expr = exprs.get(idx);
            SortDirectionType  dir  = dirs.get(idx);
            assertTrue(expr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)expr;
            String expectedNames[] = ((String)columnDescrs[2*idx]).split("\\.");
            String tableName = null;
            String columnName = null;
            if (expectedNames.length == 2) {
                tableName = expectedNames[0].toUpperCase();
                columnName = expectedNames[1].toUpperCase();
            } else {
                columnName = expectedNames[0].toUpperCase();
            }
            assertEquals(columnName, tve.getColumnName().toUpperCase());
            if (tableName != null) {
                assertEquals(tableName, tve.getTableName().toUpperCase());
            }
            assertEquals(columnDescrs[2*idx+1],       dir);
        }
    }

    /** Given a list of Class objects for plan node subclasses, asserts
     * if the given plan doesn't contain instances of those classes.
     */
    static protected void assertClassesMatchNodeChain(
            List<Class<? extends AbstractPlanNode>> expectedClasses,
            AbstractPlanNode actualPlan) {
        AbstractPlanNode pn = actualPlan;
        for (Class<? extends AbstractPlanNode> c : expectedClasses) {
            assertFalse("Actual plan shorter than expected",
                    pn == null);
            assertTrue("Expected plan to contain an instance of " + c.getSimpleName() +", "
                    + "instead found " + pn.getClass().getSimpleName(),
                    c.isInstance(pn));
            if (pn.getChildCount() > 0)
                pn = pn.getChild(0);
            else
                pn = null;
        }

        assertTrue("Actual plan longer than expected", pn == null);
    }

    /*
     * Everything below this is for generating EE Unit tests from
     * java.  We start with a newline because the licensescheck.py
     * program sometimes adds a license without a trailing newline, and
     * that make the "#include" start in the middle of a line.
     */
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

    protected String getPlanString(String sqlStmt) throws JSONException {
        AbstractPlanNode node = compile(sqlStmt);
        String planString = PlanSelector.outputPlanDebugString(node);
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
                return "voltdb::VALUE_TYPE_VARCHAR";
            }
            return "voltdb::VALUE_TYPE_" + m_type.getName();
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
     * Define a table.
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
        public TableConfig(String       tableName,
                           SchemaConfig schema,
                           Object   data[][]) {
            m_tableName   = tableName;
            m_schema      = schema;
            if (data != null) {
                m_rowCount    = data.length;
                m_data        = computeData(data);
            } else {
                m_rowCount    = 0;
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
        public TableConfig(String       tableName,
                           SchemaConfig schema,
                           int          nrows) {
            this(tableName, schema, null);
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
            int answer[][] = new int[data.length][m_schema.getNumColumns()];
            for (int ridx = 0; ridx < data.length; ridx += 1) {
                Object [] row = data[ridx];
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
                int cidx = 0;
                for (Object elem : m_data[idx]) {
                    ColumnConfig cc = m_schema.getColumn(cidx);
                    cidx += 1;
                }
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
        String       m_tableName;
        SchemaConfig m_schema;
        int          m_data[][] = null;
        List<String> m_strings = new ArrayList<>();
        int          m_rowCount;
    }

    /**
     * Define a database.
     */
    protected class DBConfig {
        DBConfig(Class<? extends PlannerTestCase>    klass,
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
            System.out.printf("Adding test %d: %s\n", m_testConfigs.size(), testConfig.m_testName);
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
            for (TestConfig tstConfig : m_testConfigs) {
                if (tstConfig.hasExpectedData()) {
                    getOneTableColumnTypesString(sb, tstConfig.getExpectedOutput());
                }
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
            for (TestConfig tstConfig : m_testConfigs) {
                if (tstConfig.hasExpectedData()) {
                    getOneTableColumnSizeString(sb, tstConfig.getExpectedOutput());
                }
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
            for (TestConfig tstConfig : m_testConfigs) {
                if (tstConfig.hasExpectedData()) {
                    getOneTableColumnNames(sb, tstConfig.getExpectedOutput());
                }
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
                                String       tableName,
                                String       rowCountName,
                                int          rowCount,
                                String       colCountName,
                                int          colCount,
                                int          data[][]) {
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
            for (TestConfig tstConfig : m_testConfigs) {
                if (tstConfig.hasExpectedData()) {
                    getOneTableStrings(sb, tstConfig.getExpectedOutput());
                }
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
            for (TestConfig tstConfig : m_testConfigs) {
                if (tstConfig.hasExpectedData()) {
                    getOneTableData(sb, tstConfig.getExpectedOutput());
                }
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
            for (TestConfig tc : m_testConfigs) {
                if (tc.hasExpectedData()) {
                    writeOneTableConfig(sb, tc.getExpectedOutput());
                }
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
                  .append("        // Plan String\n")
                  .append(String.format("        %s,\n", cleanString(getPlanString(tc.m_sqlString), "        ")))
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
                   TableConfig  expectedOutput) {
            m_testName       = testName;
            m_sqlString      = sqlString;
            m_expectedOutput = expectedOutput;
        }

        TestConfig(String testName,
                   String sqlString) {
            this(testName, sqlString, null);
        }

        /**
         * Tell whether this test has expected data.
         * @return
         */
        public boolean hasExpectedData() {
            return m_expectedOutput != null;
        }

        public TableConfig getExpectedOutput() {
            return m_expectedOutput;
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
        private String      m_testName;
        private String      m_sqlString;
        private TableConfig m_expectedOutput;
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

    private void writeFile(File path, String contents) throws Exception {
        PrintWriter out = null;
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

    private void writeTestFile(String testFolder, String testClassName, Map<String, String> params) throws Exception {
        String template = TESTFILE_TEMPLATE;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String pattern = "@" + entry.getKey() + "@";
            String value   = params.get(entry.getKey());
            template = template.replace(pattern, value);
        }
        writeFile(new File(String.format("tests/ee/%s/%s.cpp", testFolder, testClassName)), template);
    }
}
