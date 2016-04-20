package org.voltdb.planner;

/**
 * This class does not actually test much.  It is used to generate a
 * particular plan used by the C++ unit test execution/engine_test.
 * See that file for examples of how to use it.
 * 
 * If this test is changed the plan, embedded in the C++ unit test,
 * may need to be be changed as well.
 */
public class TestPlansENG10022 extends PlannerTestCase {

    public void testPlanENG10022() {
        compile("select r_customerid as cid, 2*r_customerid as cid2 from r_customer;");
    }
    
    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-eng10022.sql"), "ENG-10022", false);
    }
}
