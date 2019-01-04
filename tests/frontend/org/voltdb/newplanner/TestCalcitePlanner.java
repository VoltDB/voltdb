package org.voltdb.newplanner;

import org.voltdb.calciteadapter.CatalogAdapter;

import java.util.HashMap;
import java.util.Map;

public class TestCalcitePlanner extends CalcitePlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestVoltSqlValidator.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init(CatalogAdapter.schemaPlusFromDatabase(getDatabase()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSimpleSeqScan() {
        comparePlans("select si from R1");
    }

    public void testSeqScan() {
        comparePlans("select * from R1");
    }

    public void testSeqScanWithProjection() {
        comparePlans("select i, si from R1");
    }

    public void testSeqScanWithProjectionExpr() {
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        // in volt planner: int*int -> bigint; while in calcite: int*int -> int
        ignores.put("\"TYPE\":3,\"VALUE_TYPE\":5", "\"TYPE\":3,\"VALUE_TYPE\":6");
        comparePlans("select i * 5 from R1", ignores);
    }

    public void testSeqScanWithFilter() {
        comparePlans("select i from R1 where i = 5");
    }

    public void testSeqScanWithFilterParam() {
        comparePlans("select i from R1 where i = ? and v = ?");
    }

    public void testSeqScanWithStringFilter() {
        comparePlans("select i from R1 where v = 'FOO1'");
    }

    public void testSeqScanWithFilterWithTypeConversion() {
        // calcite adds a CAST expression to cast SMALLINT to INT
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}",
                "\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1");

        comparePlans("select i from R1 where si = 5", ignores);
    }

    public void testSeqScanWithLimit() {
        Map<String, String> ignores = new HashMap<>();
        // Inline nodes ids are swapped
        String calciteProj = "\"ID\":4,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        String voltProj = "\"ID\":3,\"PLAN_NODE_TYPE\":\"PROJECTION\"";
        ignores.put(calciteProj, voltProj);

        String calciteLimit = "\"ID\":3,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        String voltLimit = "\"ID\":4,\"PLAN_NODE_TYPE\":\"LIMIT\"";
        ignores.put(calciteLimit, voltLimit);

        comparePlans("select i from R1 limit 5", ignores);
    }
}
