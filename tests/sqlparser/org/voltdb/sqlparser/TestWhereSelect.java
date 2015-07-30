/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 *//* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sqlparser;

import static org.junit.Assert.assertFalse;
import static org.voltdb.sqlparser.symtab.CatalogAdapterAssert.assertThat;
import static org.voltdb.sqlparser.symtab.ColumnAssert.withColumnTypeNamed;
import static org.voltdb.sqlparser.symtab.TableAssert.withColumnNamed;

import java.io.IOException;

import org.junit.Test;
import org.voltdb.sqlparser.semantics.symtab.CatalogAdapter;
import org.voltdb.sqlparser.semantics.symtab.ParserFactory;
import org.voltdb.sqlparser.syntax.SQLParserDriver;
import org.voltdb.sqlparser.syntax.VoltSQLlistener;
import org.voltdb.sqlparser.syntax.grammar.ICatalogAdapter;

public class TestWhereSelect {

        VoltSQLlistener newListener() {
                CatalogAdapter catalog = new CatalogAdapter();
                ParserFactory m_factory = new ParserFactory(catalog);
        VoltSQLlistener listener = new VoltSQLlistener(m_factory);
        return listener;
    }

        @Test
    public void testDriver1() throws IOException {
                System.out.println("Test 1");
        String ddl1 = "create table alpha ( id bigint, status bigint );select id from alpha;";
        SQLParserDriver driver = new SQLParserDriver(ddl1,null);
        VoltSQLlistener listener = newListener();
        driver.walk(listener);
        assertFalse(listener.hasErrors());
        CatalogAdapter catalog = getCatalogAdapter(listener);
        assertThat(catalog).hasTableNamed("alpha",
                                          withColumnNamed("id",
                                                   withColumnTypeNamed("bigint")),
                                          withColumnNamed("status",
                                                  withColumnTypeNamed("bigint")));
    }

    @Test
    public void testDriver2() throws IOException {
                System.out.println("Test 2");
        String ddl2 = "create table alpha ( id bigint, status smallint );create table beta (id integer, status integer);\n"
                        + "select beta.id, status from alpha as mumble,beta where id = 200 AND status < 200;"; // as in voltdb, alpha.id in projection and in where clause are errors.
        VoltSQLlistener listener = newListener();
        SQLParserDriver driver = new SQLParserDriver(ddl2,listener);
        driver.walk(listener);
        assertFalse(listener.hasErrors());
    }

        @Test
    public void testDriver3() throws IOException {
                System.out.println("Test 3");
        String ddl3 = "create table alpha ( id bigint );create table beta (id bigint, local integer);\n"
                        + "select id from alpha, beta;select id from alpha,beta where beta.id < 250;";
        SQLParserDriver driver = new SQLParserDriver(ddl3,null);
        VoltSQLlistener listener = newListener();
        driver.walk(listener);
        assertFalse(listener.hasErrors());
    }

        @Test
    public void testDriver4() throws IOException {
                System.out.println("Test 4");
        String ddl4 = "create table alpha ( id bigint );create table beta (id bigint, local integer);\n"
                        + "select local, id from beta where local < 150;";
        SQLParserDriver driver = new SQLParserDriver(ddl4,null);
        VoltSQLlistener listener = newListener();
        driver.walk(listener);
        assertFalse(listener.hasErrors());
    }

        @Test
    public void testDriver5() throws IOException {
                System.out.println("Test 5");
        String ddl5 = "create table alpha ( id bigint );select id+id as alias from alpha where true = 10;";
        SQLParserDriver driver = new SQLParserDriver(ddl5,null);
        VoltSQLlistener listener = newListener();
        driver.walk(listener);
        assertFalse(listener.hasErrors());
    }

        private CatalogAdapter getCatalogAdapter(VoltSQLlistener aListener) {
            ICatalogAdapter catalog = aListener.getCatalogAdapter();
            assert(catalog instanceof CatalogAdapter);
            return (CatalogAdapter)catalog;
    }
}
