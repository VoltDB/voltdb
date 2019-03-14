package org.voltdb.plannerv2;

public class TestParser extends Plannerv2TestCase {

    private SqlParserTester m_tester = new SqlParserTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testQuotedCasing() {
        // by default quoted identifier's casing is UNCHANGED
        m_tester.sql("select \"i\" from R2").pass();
        assertEquals(m_tester.m_parsedNode.toString(),
                "SELECT `i`\n"
                + "FROM `R2`");
        m_tester.sql("select \"I\" from R2").pass();
        assertEquals(m_tester.m_parsedNode.toString(),
                "SELECT `I`\n"
                + "FROM `R2`");
    }

    public void testIdentifierQuoting() {
        // by default double quote is used as identifier delimiter
        m_tester.sql("select \"i\" from R2").pass();
        m_tester.sql("select [i] from R2").exception("Encountered \"[\" at line 1, column 8*");
        m_tester.sql("select `i` from R2").exception("Encountered \"`\" at line 1, column 8*");
    }
}