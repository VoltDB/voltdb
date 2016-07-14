package org.voltdb.planner;

import org.json_voltpatches.JSONException;
import org.voltdb.plannodes.AbstractPlanNode;

public class TestPlansEEGenerators extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {

        setupSchema(TestPlansEEGenerators.class.getResource("testplans-ee-generators.sql"),
                    "testplanseegenerator",
                    false);
    }

    public void testGeneratedPlan() throws Exception {
        String SQL[] = {


        };
        String testClass = "GeneratedEEUnitTest";
        String catalogString = getCatalogString();

        long AAA[][] = {
                { 100, 200, 300},
                { 400, 500, 600}
        };
        long BBB[][] = {
                { 1, 2, 3},
                { 4, 5, 6}
        };
        writeTestHeader(testClass, "AAA", AAA, "BBB", BBB);

        writeOneTest(testClass,
                     "testSelect",
                     "SELECT * FROM AAA ORDER BY A, B;",
                     new long [][] { { 1, 2, 3 }, { 4, 5, 6 } });

        writeTestFooter(testClass);
    }

    void writeTestHeader(String     testClass,
                         Object ... tables) {
    }

    void writeOneTest(String        testClassName,
                      String        testName,
                      String        SQL,
                      long [][]     output) throws JSONException {
        AbstractPlanNode node = compile(SQL);
        String planString = PlanSelector.outputPlanDebugString(node);
    }

    void writeTestFooter(String testClassName) {

    }
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

}
