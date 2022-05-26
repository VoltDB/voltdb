/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
 */
#ifndef TESTS_EE_TEST_UTILS_PLAN_TESTING_CONFIG_H
#define TESTS_EE_TEST_UTILS_PLAN_TESTING_CONFIG_H

/**
 * Objects of this class describe one table.  We could generate the
 * schema types, since we will know them when the tests are generated.
 * If we had C++11 we could automatically define grander classes.
 * In particular, we could generate templates with different column types.
 * But we are stuck in the past.
 */
struct TableConfig {
    /**
     * All columns of all rows have this type.
     */
    typedef     int           contentType;
    const char               *m_tableName;
    const char              **m_columnNames;
    const voltdb::ValueType  *m_types;
    const int32_t            *m_typeSizes;
    int                       m_numRows;
    int                       m_numCols;
    // Some of the ints in the m_contents are
    // really ints.  Some are offsets into the
    // string table, which is m_strings.
    const contentType        *m_contents;
    const char              **m_strings;
    const int32_t             m_numStrings;
};

/**
 * Objects of this class describe all the tables in the database.  There is
 * one of these for each test class.
 */
struct DBConfig {
    const char               *m_ddlString;
    const char               *m_catalogString;
    int                       m_numTables;
    const TableConfig       **m_tables;
};

/**
 * Objects of this class describe a single test.  All test descriptions will be contained
 * in an array of these.
 */
struct TestConfig {
    /*
     * All columns of all rows have this one type.  We could be grander,
     * either by variadic templates or else by just generating the struct
     * types.  But for now we just have single type tables.
     */
    typedef TableConfig::contentType contentType;
    const char           *m_sql;
    const bool            m_expectFail;
    const char           *m_planString;
    const TableConfig    *m_outputConfig;
};
#endif /* TESTS_EE_TEST_UTILS_PLAN_TESTING_CONFIG_H_ */
