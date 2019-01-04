package org.voltdb.newplanner;

import org.voltdb.calciteadapter.CatalogAdapter;

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
        comparePlans("select si from Ri1");
    }
}
