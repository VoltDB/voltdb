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
        compile("WITH dept_count AS (\n" +
                "    SELECT deptno, COUNT(*) AS dc\n" +
                "    FROM   employee\n" +
                "    GROUP BY deptno)\n" +
                "SELECT e.name AS employee_name,\n" +
                "       dc AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       dept_count AS ddd\n" +
                "WHERE  e.deptno = ddd.deptno;" +
                "");
    }

    public void testSimpleWithNoDot() throws Exception {
        // Use a with clause as an alias for a subquery.
        compile("WITH dept_count AS (\n" +
                "    SELECT deptno, " +
                "           COUNT(*) AS dc\n" +
                "    FROM   employee\n" +
                "    GROUP BY deptno)\n" +
                "SELECT e.name AS employee_name,\n" +
                "       dept_count.dc AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       dept_count\n" +
                "WHERE  e.deptno = deptno;" +
                "");
    }

    public void testDoubleWith() throws Exception {
        compile("WITH " +
                "  dept_count AS (\n" +
                "    SELECT deptno, " +
                "           COUNT(*) AS dept_count\n" +
                "    FROM   employee\n" +
                "    GROUP BY deptno),\n" +
                "  current_proj AS (\n" +
                "    SELECT e.empno AS empno,\n" +
                "           p.description AS pdesc\n" +
                "    FROM employee AS e,\n" +
                "         project AS p,\n" +
                "         project_participation pd\n" +
                "    WHERE     e.empno = pd.empno\n" +
                "          AND p.projectno = pd.projectno\n" +
                "          AND pd.end_date is null )\n" +
                "SELECT e.name AS employee_name,\n" +
                "       dc.dept_count AS emp_dept_count,\n" +
                "       cp.pdesc\n" +
                "FROM   employee e,\n" +
                "       dept_count dc,\n" +
                "       current_proj cp\n" +
                "WHERE      e.deptno = dc.deptno\n" +
                "       AND cp.empno = e.empno;\n" +
                "");
    }

    public void testWithInFrom() throws Exception {
        compile("SELECT e.name AS employee_name, \n" +
                "       dc.dept_count AS emp_dept_count \n" +
                "FROM   employee e, \n" +
                "       (WITH " +
                "          dept_count AS ( \n" +
                "            SELECT deptno, COUNT(*) AS dept_count \n" +
                "            FROM   employee \n" +
                "            GROUP BY deptno ) \n" +
                "        select * FROM dept_count) AS dc \n" +
                "WHERE  e.deptno = dc.deptno; \n" +
                "");
    }
    public void testWithInIn() throws Exception {
        // Note that dept_count is an alias for a subquery and
        // an alias for a column in that subquery.  While this
        // might be worst practice in practice, in a test it
        // makes sense, because we want to test that HSQLDB
        // gets the scope rules right.
        compile("SELECT e.name AS employee_name\n " +
                "FROM   employee e\n " +
                "WHERE  e.deptno IN ( WITH dept_count AS (\n " +
                "                        SELECT deptno, " +
                "                               COUNT(*) AS dept_count\n " +
                "                        FROM   employee emp\n " +
                "                        GROUP BY deptno\n " +
                "                        HAVING count(*) > 5)\n " +
                "                     SELECT dc.deptno FROM dept_count as dc);\n " +
                "");


    }

    public void testWithInExists() throws Exception {
        // The alias "dept_count as dc" is logically unnecessary in the
        // main select of the argument to exists.  Again, we are pushing
        // HSQLDB's scope envelope a bit with WITH clauses.
        compile("SELECT e.name AS employee_name\n" +
                "FROM   employee e\n" +
                "WHERE  exists ( WITH dept_count AS (\n" +
                "                  SELECT deptno," +
                "                         COUNT(*) AS dept_size\n" +
                "                  FROM   employee\n" +
                "                  GROUP BY deptno\n" +
                "                  HAVING count(*) > 5 )\n" +
                "               SELECT dc.deptno FROM dept_count AS dc);\n" +
                "");
        // This is the same query as above, but without the logically unnecessary
        // alias.
        compile("SELECT e.name AS employee_name\n" +
                "FROM   employee e\n" +
                "WHERE  exists ( WITH dept_count AS (\n" +
                "                  SELECT deptno, \n" +
                "                         COUNT(*) AS dept_size\n" +
                "                  FROM   employee\n" +
                "                  GROUP BY deptno\n" +
                "                  HAVING count(*) > 5)\n" +
                "                SELECT deptno FROM dept_count);\n" +
                "");
    }

    /**
     * This is here only because I want to compare withs and subqueries.
     * It should be an inessential variation of the multiple-with test.
     *
     * @throws Exception
     */
    public void testSubquery1() throws Exception {
        compile("SELECT e.name AS employee_name,\n" +
                "       dc.dept_count AS emp_dept_count,\n" +
                "       cp.pdesc AS Project_Description\n" +
                "FROM   employee e,\n" +
                "       (SELECT deptno,\n" +
                "               COUNT(*) AS dept_count\n" +
                "        FROM   employee\n" +
                "        GROUP BY deptno) AS dc,\n" +
                "       (SELECT e.empno AS empno,\n" +
                "               p.description AS pdesc\n" +
                "        FROM employee AS e,\n" +
                "             project  AS p,\n" +
                "             project_participation AS pd\n" +
                "        WHERE e.empno = pd.empno\n" +
                "        AND   p.projectno = pd.projectno\n" +
                "        AND   pd.end_date IS NULL) AS cp\n" +
                "WHERE  e.deptno = deptno\n" +
                "and    cp.empno = e.empno;\n" +
                "");
    }

    public void testSubquery2() throws Exception {
        compile("SELECT e.name AS employee_name,\n" +
                "       dc.deptno AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       (SELECT *\n" +
                "         FROM   employee) dc\n" +
                "WHERE  e.deptno = dc.deptno;\n" +
                "");
    }
}
