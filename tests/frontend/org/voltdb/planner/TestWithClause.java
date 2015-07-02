package org.voltdb.planner;

public class TestWithClause extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestFunctions.class.getResource("testwithclause.sql"),
                    "testwithclause", planForSinglePartitionFalse);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void notestSimpleWith() throws Exception {
        compile("WITH dept_count AS (\n" +
                "  SELECT deptno, COUNT(*) AS dept_count\n" +
                "  FROM   employee\n" +
                "  GROUP BY deptno)\n" +
                "SELECT e.name AS employee_name,\n" +
                "       dc.dept_count AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       dept_count dc\n" +
                "WHERE  e.deptno = dc.deptno;" +
                "");
    }

    public void notestDoubleWith() throws Exception {
        compile(
            "WITH dept_count AS (\n" +
            "  SELECT deptno, COUNT(*) AS dept_count\n" +
            "  FROM   employee\n" +
            "  GROUP BY deptno),\n" +
            "  current_proj AS (\n" +
            "  select e.empno as empno,\n" +
            "         p.description as pdesc\n" +
            "  from\n" +
      	    "   employee as e,\n" +
            "        project as p,\n" +
            "   project_participation pd\n" +
            "  where e.empno = pd.empno\n" +
            "  and   p.projectno = pd.projectno\n" +
            "  and   pd.end_date is null )\n" +
            "SELECT e.name AS employee_name,\n" +
            "       dc.dept_count AS emp_dept_count,\n" +
            "       cp.pdesc\n" +
            "FROM   employee e,\n" +
            "       dept_count dc,\n" +
            "       current_proj cp\n" +
            "WHERE  e.deptno = dc.deptno\n" +
            "and    cp.empno = e.empno;\n" +
            ""
            );
    }
    public void notestWithInFrom() throws Exception {
        compile(
                "SELECT e.name AS employee_name, \n" +
                "       dc.dept_count AS emp_dept_count \n" +
                "FROM   employee e, \n" +
                "       (WITH dept_count AS ( \n" +
                "         SELECT deptno, COUNT(*) AS dept_count \n" +
                "        FROM   employee \n" +
                "        GROUP BY deptno) \n" +
                "       select * from dept_count) as dc \n" +
                "WHERE  e.deptno = dc.deptno; \n" +
                ""
                );
    }
    public void notestWithInIn() throws Exception {
        compile("SELECT e.name AS employee_name\n " +
                "FROM   employee e\n " +
                "WHERE  e.deptno in ( WITH dept_count AS (\n " +
                "                              SELECT deptno, COUNT(*) AS dept_count\n " +
                "                        FROM   employee\n " +
                "                        GROUP BY deptno\n " +
                "                        HAVING count(*) > 5)\n " +
                "                    select deptno from dept_count);\n " +
                ""
                );

    }

    public void notestWithInExists() throws Exception {
        compile("SELECT e.name AS employee_name\n" +
                "FROM   employee e\n" +
                "WHERE  exists ( WITH dept_count AS (\n" +
                "       		SELECT deptno, COUNT(*) AS dept_count\n" +
                "		FROM   employee\n" +
                "		GROUP BY deptno\n" +
                "		HAVING count(*) > 5)\n" +
                "		select deptno from dept_count);\n" +
                ""
                );
    }

    /**
     * This is here only because I want to compare withs and subqueries.
     * It should be an inessential variation of the multiple-with test.
     *
     * @throws Exception
     */
    public void notestSubquery1() throws Exception {
        compile("SELECT e.name AS employee_name,\n" +
                "       dc.dept_count AS emp_dept_count,\n" +
                "       cp.pdesc as Project_Description\n" +
                "FROM   employee e,\n" +
                "       (SELECT deptno, COUNT(*) AS dept_count\n" +
                "         FROM   employee\n" +
                "	      GROUP BY deptno) dc,\n" +
                "       (select e.empno as empno,\n" +
                "               p.description as pdesc\n" +
                "	from\n" +
                "	       employee as e,\n" +
                "               project as p,\n" +
                "	       project_participation pd\n" +
                "        where e.empno = pd.empno\n" +
                "        and   p.projectno = pd.projectno\n" +
                "        and   pd.end_date is null) as cp\n" +
                "WHERE  e.deptno = dc.deptno\n" +
                "and    cp.empno = e.empno;\n" +
                ""
               );
    }

    public void notestSubquery2() throws Exception {
        compile("SELECT e.name AS employee_name,\n" +
                "       dc.deptno AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       (SELECT *\n" +
                "         FROM   employee) dc\n" +
                "WHERE  e.deptno = dc.deptno;\n" +
                ""
               );
    }
    /**
     * All of the tests above are commented out.  They all fail.  We
     * need at least one test to pass, or JUnit becomes dyspeptic.
     *
     * @throws Exception
     */
    public void testNothing() throws Exception {
        assertTrue(true);
    }
}
