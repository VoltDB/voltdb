/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package org.voltdb.planner;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;

public class TestPlansCommonTableExpression extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-cte.sql"),
                    "testcte", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }


    public void testPlansWith() {
        String SQL = "WITH RT(ID, NAME) AS "
                     + "("
                     + "  SELECT ID, NAME FROM CTE_TABLE WHERE ID = ?"
                     + "    INTERSECT "
                     + "  SELECT CTE_TABLE.ID, CTE_TABLE.NAME "
                     + "  FROM CTE_TABLE "
                     + ") "
                     + "SELECT * FROM RT;";
        try {
            VoltXMLElement xml = compileToXML(SQL);
            System.out.println(xml.toXML());
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        }
    }
    public void testPlansCTE() {
        String SQL = "WITH RECURSIVE RT(ID, NAME) AS "
                     + "("
                     + "  SELECT ID, NAME FROM CTE_TABLE WHERE ID = ?"
                     + "    UNION ALL "
                     + "  SELECT RT.ID, RT.NAME "
                     + "  FROM RT JOIN CTE_TABLE "
                     + "          ON RT.ID IN (CTE_TABLE.LEFT_RENT, CTE_TABLE.RIGHT_RENT)"
                     + ") "
                     + "SELECT * FROM RT;";
        try {
            VoltXMLElement xml = compileToXML(SQL);
            System.out.println(xml.toXML());
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        }
    }
    public void testPlansMultiCTE() {
        String SQL = "WITH RECURSIVE "
                     + "ST (mumble, bazzle) AS ( "
                     + "  SELECT ID, NAME FROM CTE_TABLE WHERE ID = ?"
                     + "    UNION ALL "
                     + "  SELECT L.MUMBLE, L.BAZZLE "
                     + "  FROM ST L JOIN CTE_TABLE R "
                     + "          ON L.MUMBLE IN (R.LEFT_RENT, R.RIGHT_RENT)"
                     + "), "
                     + "RT(ID, NAME) AS ( "
                     + "  SELECT ID, NAME FROM CTE_TABLE WHERE ID = ?"
                     + "    UNION ALL "
                     + "  SELECT RT.ID, RT.NAME "
                     + "  FROM RT JOIN CTE_TABLE "
                     + "          ON RT.ID IN (CTE_TABLE.LEFT_RENT, CTE_TABLE.RIGHT_RENT)"
                     + ") "
                     + "SELECT * FROM RT;";
        try {
            VoltXMLElement xml = compileToXML(SQL);
            System.out.println(xml.toXML());
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        }
    }

}
