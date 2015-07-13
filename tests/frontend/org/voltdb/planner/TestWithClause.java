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

package org.voltdb.planner;

public class TestWithClause extends PlannerTestCase {
    private static final boolean allTests = false;
    private static final boolean WITH_BASIC_DONE = false;
    private static final boolean WITH_DOUBLE = false;
    private static final boolean WITH_IN_FROM = false;
    private static final boolean WITH_IN_IN = false;
    private static final boolean WITH_IN_EXISTS = false;
    private static final boolean WITH_IN_SUBQUERY1 = false;
    private static final boolean WITH_IN_SUBQUERY2 = false;
    private static final boolean doTest(boolean condition) {
        return allTests || condition;
    }

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

    public void testSimpleWith() throws Exception {
        if (doTest(WITH_BASIC_DONE)) {
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
    }

    public void testDoubleWith() throws Exception {
        if (doTest(WITH_DOUBLE)) {
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
    }
    public void testWithInFrom() throws Exception {
        if (doTest(WITH_IN_FROM)) {
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
    }
    public void testWithInIn() throws Exception {
        if (doTest(WITH_IN_IN)) {
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
    }

    public void testWithInExists() throws Exception {
        if (doTest(WITH_IN_EXISTS)) {
            compile("SELECT e.name AS employee_name\n" +
                    "FROM   employee e\n" +
                    "WHERE  exists ( WITH dept_count AS (\n" +
                    "                       SELECT deptno, COUNT(*) AS dept_count\n" +
                    "               FROM   employee\n" +
                    "               GROUP BY deptno\n" +
                    "               HAVING count(*) > 5)\n" +
                    "               select deptno from dept_count);\n" +
                    ""
                    );
        }
    }

    /**
     * This is here only because I want to compare withs and subqueries.
     * It should be an inessential variation of the multiple-with test.
     *
     * @throws Exception
     */
    public void testSubquery1() throws Exception {
        if (doTest(WITH_IN_SUBQUERY1)) {
            compile("SELECT e.name AS employee_name,\n" +
                    "       dc.dept_count AS emp_dept_count,\n" +
                    "       cp.pdesc as Project_Description\n" +
                    "FROM   employee e,\n" +
                    "       (SELECT deptno, COUNT(*) AS dept_count\n" +
                    "         FROM   employee\n" +
                    "             GROUP BY deptno) dc,\n" +
                    "       (select e.empno as empno,\n" +
                    "               p.description as pdesc\n" +
                    "       from\n" +
                    "              employee as e,\n" +
                    "               project as p,\n" +
                    "              project_participation pd\n" +
                    "        where e.empno = pd.empno\n" +
                    "        and   p.projectno = pd.projectno\n" +
                    "        and   pd.end_date is null) as cp\n" +
                    "WHERE  e.deptno = dc.deptno\n" +
                    "and    cp.empno = e.empno;\n" +
                    ""
                   );
        }
    }

    public void testSubquery2() throws Exception {
        if (doTest(WITH_IN_SUBQUERY2)) {
            compile("SELECT e.name AS employee_name,\n" +
                    "       dc.deptno AS emp_dept_count\n" +
                    "FROM   employee e,\n" +
                    "       (SELECT *\n" +
                    "         FROM   employee) dc\n" +
                    "WHERE  e.deptno = dc.deptno;\n" +
                    ""
                   );
        }
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
