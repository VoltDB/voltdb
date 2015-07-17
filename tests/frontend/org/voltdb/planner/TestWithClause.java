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
    // hsql232 ENG-8626
    private static final boolean HSQL232_ENG_8626_DONE = false;
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

    private final void doCompileTest(boolean enabled, String sql) {
        if (doTest(enabled)) {
            compile(sql);
        } else {
            failToCompile(sql);
        }
    }

    public void testSimpleWith() throws Exception {
        compile("WITH dept_count AS (\n" +
                "  SELECT deptno, COUNT(*) AS dc\n" +
                "  FROM   employee\n" +
                "  GROUP BY deptno)\n" +
                "SELECT e.name AS employee_name,\n" +
                "       dc AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       dept_count as ddd\n" +
                "WHERE  e.deptno = ddd.deptno;" +
                "");
    }

    public void testSimpleWithNoDot() throws Exception {
        // This seems like a plausible query.  The unqualified variable
        // dept_no in the where clause can only be satisfied by the
        // table dept_count.  But HSQLDB tells us every derived table
        // needs an alias, including dept_count in the FROM clause.
        // I think this is an error, and is discussed in ENG-8638.
        //
        compile("WITH dept_count AS (\n" +
                "  SELECT deptno, COUNT(*) as dc\n" +
                "  FROM   employee\n" +
                "  GROUP BY deptno)\n" +
                "SELECT e.name AS employee_name,\n" +
                "       dept_count.dc AS emp_dept_count\n" +
                "FROM   employee e,\n" +
                "       dept_count\n" +
                "WHERE  e.deptno = deptno;" +
                "");
    }

    public void testDoubleWith() throws Exception {
        compile("WITH dept_count AS (\n" +
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
                "");
    }

    public void testWithInFrom() throws Exception {
        compile("SELECT e.name AS employee_name, \n" +
                "       dc.dept_count AS emp_dept_count \n" +
                "FROM   employee e, \n" +
                "       (WITH dept_count AS ( \n" +
                "         SELECT deptno, COUNT(*) AS dept_count \n" +
                "        FROM   employee \n" +
                "        GROUP BY deptno) \n" +
                "       select * from dept_count) as dc \n" +
                "WHERE  e.deptno = dc.deptno; \n" +
                "");
    }
    public void testWithInIn() throws Exception {
        // Logically the "dc.deptno" and "as dc" in the RHS of the
        // "in" operation is not necessary.  However, HSQLDB gives
        // an error without it.  When ENG-8638 is
        compile("SELECT e.name AS employee_name\n " +
                "FROM   employee e\n " +
                "WHERE  e.deptno in ( WITH dept_count AS (\n " +
                "                              SELECT deptno, COUNT(*) AS dept_count\n " +
                "                        FROM   employee emp\n " +
                "                        GROUP BY deptno\n " +
                "                        HAVING count(*) > 5)\n " +
                "                    select dc.deptno from dept_count as dc);\n " +
                "");
        // This is very close to the previous query.  But it does not have
        // the logically unnecessary alias for dept_count.
        compile("SELECT e.name AS employee_name\n " +
                "FROM   employee e\n " +
                "WHERE  e.deptno in ( WITH dept_count AS (\n " +
                "                              SELECT deptno\n " +
                "                        FROM   employee emp\n " +
                "                        GROUP BY deptno\n " +
                "                        HAVING count(*) > 5)\n " +
                "                    select deptno from dept_count);\n " +
                "");

        // This fails because the From clause in the main select statement
        // references the with-view named "dept_ident" without an alias.
        // This is an HSQLDB error condition which may be incorrect.
        // However, if we give it an alias, then the compilation fails
        // because the parenthesized expression "e.deptno in (deptno)"
        // generates a VALUELIST which we don't know what to do with.
        doCompileTest(HSQL232_ENG_8626_DONE,
                      "WITH dept_ident AS (\n" +
                      "  SELECT deptno \n" +
                      "  FROM   employee\n" +
                      "  GROUP BY deptno)\n" +
                      "SELECT e.name AS employee_name\n" +
                      "FROM   employee e,\n" +
                      "       dept_ident\n" +
                      "WHERE  e.deptno IN ( deptno );" +
                      "");

        // This fails because the WHERE expression "e.deptno IN ( di.deptno )"
        // generates a tree with a VALUELIST in the HSQLDB AST, and we cannot
        // translate it yet.  This is the complaint of ENG-8626.
        doCompileTest(HSQL232_ENG_8626_DONE,
                      "WITH dept_ident AS (\n" +
                      "  SELECT deptno \n" +
                      "  FROM   employee\n" +
                      "  GROUP BY deptno)\n" +
                      "SELECT e.name AS employee_name\n" +
                      "FROM   employee e,\n" +
                      "       dept_ident as di\n" +
                      "WHERE  e.deptno IN ( di.deptno );" +
                      "");

    }

    public void testWithInExists() throws Exception {
        // The alias "dept_count as dc" is logically unnecessary in the
        // main select of the argument to exists.  It is required by
        // the HSQLDB bug ENG-8638.
        compile("SELECT e.name AS employee_name\n" +
                "FROM   employee e\n" +
                "WHERE  exists ( WITH dept_count AS (\n" +
                "                       SELECT deptno, COUNT(*) AS dept_size\n" +
                "               FROM   employee\n" +
                "               GROUP BY deptno\n" +
                "               HAVING count(*) > 5)\n" +
                "               select dc.deptno from dept_count as dc);\n" +
                "");
        // This is the same query as above, but without the logically unnecessary
        // alias.
        compile("SELECT e.name AS employee_name\n" +
                "FROM   employee e\n" +
                "WHERE  exists ( WITH dept_count AS (\n" +
                "                       SELECT deptno, COUNT(*) AS dept_size\n" +
                "               FROM   employee\n" +
                "               GROUP BY deptno\n" +
                "               HAVING count(*) > 5)\n" +
                "               select deptno from dept_count);\n" +
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
