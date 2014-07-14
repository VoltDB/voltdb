/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

public class TestInsertIntoSelect extends PlannerTestCase {

        @Override
        protected void setUp() throws Exception {
                setupSchema(TestInsertIntoSelect.class.getResource("testplans-insertintoselect-ddl.sql"),
                        "insertintoselect", false);
            }

            @Override
            protected void tearDown() throws Exception {
                super.tearDown();
            }

            public void testSupportedCases() {

                // TODO: check plans

                // This works
                compile("insert into target_p " +
                                        "select * from source_p1");

                // Columns can also be explicitly specified
                compile("insert into target_p (bi, vc, ii, ti) select * from source_p1");

                // a join in the select works ok too.
                compile("insert into target_p " +
                                        "select sp1.bi, sp1.vc, sp2.ii, sp2.ti " +
                                        "from source_p1 as sp1 inner join source_p2 as sp2 on sp1.bi = sp2.bi " +
                                        "where sp1.bi between ? and ?;");
                // no unions allowed yet!  Seems like this could be supported at some point?
                // - for inserting into replicated tables, its ok as long as all tables in the query expression are also replicated
            }

            public void testShouldBeSupportedCases() {

                // defaults
                failToCompile("insert into target_p (bi, ii) select bi, ii from source_p1",
                                "Default values are not supported in 'INSERT INTO SELECT FROM'. " +
                                "Each column in table 'TARGET_P' must be provided an explicit value.");

                // casting between types.  message could be better
                failToCompile("insert into target_p (bi, vc, ii, ti) select bi, vc, ti, ii from source_p1",
                                "Column 'II' in 'INSERT ONTO SELECT FROM'. for table 'TARGET_P' " +
                                "must be set to value of the same exact type.");

                // no unions allowed yet!  Seems like this could be supported at some point?
                // - for inserting into replicated tables, its ok as long as all tables in the query expression are also replicated
                failToCompile("insert into target_p " +
                                                "select * from source_p1 union select * from source_r",
                                                "The table 'TARGET_P' must not be updated " +
                                                "directly from a UNION or other set operation.");

                // re-ordered columns
                failToCompile("insert into target_p (vc, bi, ii, ti) select vc, bi, ii, ti from source_p1",
                                "Columns in 'INSERT ONTO SELECT FROM'. for table 'TARGET_P' " +
                                "must be listed in the order that they were defined.");

                // insert into select, where both sources and targets are replicated.  Should be ok!
                failToCompile("insert into target_r " +
                                                "select * from source_r union select * from source_r",
                                                "The table 'TARGET_R' must not be updated directly from a UNION or other set operation.");
            }


            public void testUnsupportedCases() {

                // inserting into a replicated table from a partitioned one is not allowed.
                failToCompile("insert into target_r select * from source_p1",
                                          "The replicated table, 'TARGET_R', " +
                                          "must not be the recipient of partitioned data in a single statement.  " +
                                          "Use separate INSERT and SELECT statements, optionally within a stored procedure.");

                // Partitioning column not assigned
                failToCompile("insert into target_p (vc, ii) select vc, ii from source_p1",
                                "The partitioning column 'BI' of the partitioned table 'TARGET_P' " +
                                "must be explicitly set to a partitioning column in the SELECT clause. " +
                                "Use separate INSERT and SELECT statements, optionally within a stored procedure.");

                // partitioning column assigned by a non-partitioning column
                failToCompile("insert into target_p (bi, vc, ii, ti) select bi + 10, vc, ii, ti from source_p1",
                                "The partitioning column 'BI' of the partitioned table 'TARGET_P' " +
                                "must be explicitly set to a partitioning column in the SELECT clause. " +
                                "Use separate INSERT and SELECT statements, optionally within a stored procedure.");
            }
}
