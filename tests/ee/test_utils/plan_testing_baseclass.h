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

#pragma once

#include <cstdlib>
#include <sstream>
#include <string.h>

#include "catalog/cluster.h"
#include "catalog/constraint.h"
#include "catalog/table.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/SynchronizedThreadLock.h"
#include "execution/VoltDBEngine.h"
#include "storage/temptable.h"
#include "test_utils/LoadTableFrom.hpp"
#include "test_utils/plan_testing_config.h"

typedef int64_t fragmentId_t;

/**
 * This Topend allows us to get fragments by fragment id.  Other
 * than that, this is just a DummyTopend.  This is used mostly
 * when executing fragments during EE testing.
 */
class EngineTestTopend : public voltdb::DummyTopend {
    typedef std::map<fragmentId_t, std::string> fragmentMap;
    fragmentMap m_fragments;
public:
    static EngineTestTopend *newInstance() {
        return new EngineTestTopend();
    }

    void addPlan(fragmentId_t fragmentId, const std::string &planStr) {
        m_fragments[fragmentId] = planStr;
    }
    std::string planForFragmentId(fragmentId_t fragmentId) {
        fragmentMap::iterator it = m_fragments.find(fragmentId);
        if (it == m_fragments.end()) {
            return "";
        } else {
            return it->second;
        }
    }
};

/**
 * This base class is useful for tests which execute entire plans.  These
 * are useful for debugging executors in their native habitats.
 */
template <typename TOPEND>
class PlanTestingBaseClass : public Test {
public:
    /**
     * This needs a catalog string and a random seed.  The random seed is
     * used to generate initial data for the table.
     */
    PlanTestingBaseClass()
        : m_cluster_id(1),
        m_database_id(0),
        m_site_id(1),
        m_catalog(NULL),
        m_cluster(NULL),
        m_database(NULL),
        m_constraint(NULL),
        m_isinitialized(false),
        m_fragmentNumber(100),
        m_paramCount(0)
    { }

    void initialize(const char   *catalogString,
                    uint32_t  randomSeed = (uint32_t)time(NULL)) {
        initialize(catalogString, 0, NULL, randomSeed);
    }

    void initialize(const DBConfig  &db,
                    uint32_t  randomSeed = (uint32_t)time(NULL)) {
        initialize(db.m_catalogString, db.m_numTables, db.m_tables, randomSeed);
    }
    void initialize(const char         *catalogString,
                    int                 numTables,
                    const TableConfig **tables,
                    uint32_t            randomSeed) {
        srand(randomSeed);
        m_catalog_string = catalogString;
        /*
         * Initialize the engine.  We create our own
         * topend, to make sure we can supply fragments
         * by id, and then make sure we know where the
         * shared buffers are.  Note that calling setBuffers
         * sets the shared buffer pointers, and calling
         * resetReusedResultOutputBuffer causes the engine to
         * use them.
         */
        m_topend.reset(TOPEND::newInstance());
        m_engine.reset(new voltdb::VoltDBEngine(m_topend.get()));

        m_parameter_buffer.reset(new char[m_smallBufferSize]);
        ::memset(m_parameter_buffer.get(), 0, m_smallBufferSize);

        m_per_fragment_stats_buffer.reset(new char[m_smallBufferSize]);
        ::memset(m_per_fragment_stats_buffer.get(), 0, m_smallBufferSize);

        m_result_buffer.reset(new char[m_resultBufferSize]);
        ::memset(m_result_buffer.get(), 0, m_resultBufferSize);

        m_exception_buffer.reset(new char[m_smallBufferSize]);
        ::memset(m_exception_buffer.get(), 0, m_smallBufferSize);

        m_engine->setBuffers(m_parameter_buffer.get(), m_smallBufferSize,
                             m_per_fragment_stats_buffer.get(), m_smallBufferSize,
                             NULL, 0, // the UDF buffer
                             NULL, 0, // the first result buffer
                             m_result_buffer.get(), m_resultBufferSize,
                             m_exception_buffer.get(), m_smallBufferSize);
        m_engine->resetReusedResultOutputBuffer();
        m_engine->resetPerFragmentStatsOutputBuffer();
        int partitionCount = 1;
        m_engine->initialize(m_cluster_id, m_site_id, 0, partitionCount, 0, "", 0, 1024, false, -1, false, voltdb::DEFAULT_TEMP_TABLE_MEMORY, true);
        partitionCount = htonl(partitionCount);
        int tokenCount = htonl(100);
        int partitionId = htonl(0);

        int data[3] = {partitionCount, tokenCount, partitionId};
        m_engine->updateHashinator((char*)data, NULL, 0);
        ASSERT_TRUE(m_engine->loadCatalog( -2, m_catalog_string));

        /*
         * Get a link to the catalog and pull out information about it
         */
        m_catalog = m_engine->getCatalog();
        m_cluster = m_catalog->clusters().get("cluster");
        m_database = m_cluster->databases().get("database");
        m_database_id = m_database->relativeIndex();
        ASSERT_NE(NULL, m_catalog);
        ASSERT_NE(NULL, m_cluster);
        ASSERT_NE(NULL, m_database);
        ASSERT_LT(0,    m_database_id);

        m_isinitialized = true;
        for (int idx = 0; idx < numTables; idx += 1) {
            initTable(tables[idx]);
        }
    }
    ~PlanTestingBaseClass() {
        //
        // When we delete the VoltDBEngine
        // it will cleanup all the tables for us.
        // The m_pool will delete all of its memory
        // as well.
        //
        m_engine.reset();
        voltdb::globalDestroyOncePerProcess();
    }

    voltdb::PersistentTable *getPersistentTableAndId(const std::string &name,
                                                     int *id,
                                                     voltdb::PersistentTable **table) {
        catalog::Table *tbl = m_database->tables().get(name);
        assert(tbl != NULL);
        if (id != NULL) {
            *id = tbl->relativeIndex();
        }
        voltdb::PersistentTable *pt = dynamic_cast<voltdb::PersistentTable *>(m_engine->getTableByName(name));
        if (table != NULL) {
            *table = pt;
        }
        return pt;
    }

    // The first two parameters are solely out parameters.
    // They may be NULL if their value is not wanted.
    void initTable(voltdb::PersistentTable       **table,
                   int                            *table_id,
                   const TableConfig              *oneTable) {
        initializeTable(oneTable->m_tableName,
                        table,
                        table_id,
                        oneTable->m_types,
                        oneTable->m_typeSizes,
                        oneTable->m_numRows,
                        oneTable->m_numCols,
                        oneTable->m_contents,
                        oneTable->m_strings,
                        oneTable->m_numStrings);
    }

    void initTable(const TableConfig *oneTable) {
        // We apparently don't want the persistent table or
        // the table id.
        initTable(NULL, NULL, oneTable);
    }

    void initializeTable(const std::string               tableName,
                         voltdb::PersistentTable       **table,
                         int                            *tableId,
                         const voltdb::ValueType        *types,
                         const int32_t                  *typesizes,
                         int                             nRows,
                         int                             nCols,
                         const int32_t                  *vals,
                         const char                    **strings,
                         int                             num_strings) {
        voltdb::PersistentTable *pTable = getPersistentTableAndId(tableName.c_str(), tableId, table);
        if (pTable == NULL) {
            std::ostringstream oss;
            oss << "Cannot find table "
                << tableName
                << " in the schema."
                << std::endl;
            throw std::logic_error(oss.str());
        }
        assert(pTable != NULL);
        voltdb::ConditionalSynchronizedExecuteWithMpMemory setMpMemoryIfNeeded
                (pTable->isReplicatedTable(), true, [](){});
        for (int row = 0; row < nRows; row += 1) {
            if (row > 0 && (row % 100 == 0)) {
                std::cout << '.';
                std::cout.flush();
            }
            voltdb::TableTuple &tuple = pTable->tempTuple();
            for (int col = 0; col < nCols; col += 1) {
                int val;
                std::string strstr;
                if (vals != NULL) {
                    // If we have values, then use them.
                    val = vals[(row*nCols) + col];
                    if (types[col] == voltdb::ValueType::tVARCHAR) {
                        if (val < 0 || (num_strings <= val)) {
                            std::ostringstream oss;
                            oss << "string index "
                                << val
                                << " out of range [0, "
                                << num_strings
                                << "]"
                                << std::endl;
                            throw std::logic_error(oss.str());
                        }
                        voltdb::NValue nval = voltdb::ValueFactory::getStringValue(strings[val], &m_pool);
                        tuple.setNValue(col, nval);
                    } else {
                        voltdb::NValue nval = voltdb::ValueFactory::getIntegerValue(val);
                        tuple.setNValue(col, nval);
                    }
                } else {
                    // If we have no values, generate them randomly.
                    if (types[col] == voltdb::ValueType::tVARCHAR) {
                        strstr = getRandomString(1, typesizes[col]);
                        voltdb::NValue nval = voltdb::ValueFactory::getStringValue(strstr.c_str(), &m_pool);
                        tuple.setNValue(col, nval);
                    } else {
                        val = getRandomInt(0, typesizes[col]);
                        voltdb::NValue nval = voltdb::ValueFactory::getIntegerValue(val);
                        tuple.setNValue(col, nval);
                    }
                }
            }
            if (!pTable->insertTuple(tuple)) {
                return;
            }
        }
        if (nRows > 100) {
            std::cout << std::endl;
        }
    }

    /**
     * Get a random integer in the range [minval, maxval).
     * The distribution is whatever std::random uses, which
     * should be uniform.
     */
    const int32_t getRandomInt(int minval, int maxval) {
        if (maxval < 0 || maxval <= minval) {
            return minval;
        }
        double r = (maxval - 1 - minval)/static_cast<double>(RAND_MAX);
        return static_cast<int32_t>(std::rand() * r) + minval;
    }

    /**
     * Get a random string whose length is between minlen and
     * maxlen.  The characters are all upper or lower case, or
     * digits.
     */
    const std::string getRandomString(int minlen, int maxlen) {
        std::stringstream ostr;
        int len = getRandomInt(minlen, maxlen+1);
        for (int idx = 0; idx < len; idx += 1) {
            int32_t rv = getRandomInt(0, 62);
            char ch;
            if (rv < 26) {
                ch = rv + 'a';
            } else if (rv < 36) {
                ch = rv - 26 + '0';
            } else {
                ch = rv - 36 + 'A';
            }
            ostr << ch;
        }
        return ostr.str();
    }
    /**
     * Execute a single test.  Execute the test's fragment, and then
     * validate the output table.
     */
    void executeTest(const TestConfig &test) {
        // The fragment number doesn't really matter here.
        executeFragment(m_fragmentNumber, test.m_planString);
        // If we have expected output data, then validate it.
        if (test.m_outputConfig != NULL) {
            validateResult(test.m_outputConfig, test.m_expectFail);
        }
    }
    /**
     * Given a PlanFragmentInfo data object, make the m_engine execute it,
     * and validate the results.
     */
    void executeFragment(fragmentId_t fragmentId, const char *plan) {
        m_topend->addPlan(fragmentId, plan);

        // Make sure the parameter buffer is filled
        // with healthful zeros, and then create an input
        // deserializer.
        memset(m_parameter_buffer.get(), 0, m_smallBufferSize);
        voltdb::ReferenceSerializeInputBE emptyParams(m_parameter_buffer.get(), m_smallBufferSize);

        //
        // Execute the plan.  You'd think this would be more
        // impressive.
        //
        try {
            m_engine->executePlanFragments(1, &fragmentId, NULL, emptyParams, 1000, 1000, 1000, 1000, 1, false);
        } catch (voltdb::SerializableEEException &ex) {
            throw;
        }
    }

    /**
     * Fetch the results.  We have forced them to be written
     * to our own buffer in the local engine.  But we don't
     * know how much of the buffer is actually used.  So we
     * need to query the engine.
     */
    void validateResult(const TableConfig *answer, bool expectFail) {
        int nRows = answer->m_numRows;
        int nCols = answer->m_numCols;
        size_t result_size = m_engine->getResultsSize();
        boost::scoped_ptr<voltdb::TempTable> result(voltdb::loadTableFrom(m_result_buffer.get(), result_size));
        assert(result.get() != NULL);
        ASSERT_TRUE(result != NULL);

        const voltdb::TupleSchema* res_schema = result->schema();
        voltdb::TableTuple tuple(res_schema);
        voltdb::TableIterator iter = result->iterator();
        if (!iter.hasNext() && nRows > 0) {
            printf("No results!!\n");
            ASSERT_FALSE(true);
        }
        int32_t resColCount = result->columnCount();
        if (nCols != resColCount) {
            std::ostringstream oss;
            oss << "Error: nCols = "
                << nCols
                << " != resColCount = "
                << resColCount
                << "."
                << std::endl;
        }
        ASSERT_EQ(nCols, resColCount);
        bool failed = false;
        for (int32_t row = 0; row < nRows; row += 1) {
            ASSERT_TRUE(iter.next(tuple));
            for (int32_t col = 0; col < nCols; col += 1) {
                int32_t expected = answer->m_contents[row * nCols + col];
                if (answer->m_types[col] == voltdb::ValueType::tVARCHAR) {
                    voltdb::NValue nval = tuple.getNValue(col);
                    int32_t actualSize;
                    const char *actualStr = voltdb::ValuePeeker::peekObject(nval, &actualSize);
                    if (actualStr == NULL) {
                        actualSize = -1;
                    }
                    const char *expStr = answer->m_strings[expected];
                    size_t expSize = (expStr != NULL) ? strlen(expStr) : -1;
                    bool neq = (((actualStr == NULL) != (expStr == NULL))
                                || (actualSize != expSize)
                                || ((actualStr != NULL)
                                    && (expStr != NULL)
                                    && (::strncmp(actualStr, expStr, actualSize) != 0)));
                    VOLT_TRACE("Row %02d, col %02d: expected \"%s\", got \"%s\" (%s)",
                               row, col,
                               expStr, actualStr,
                               neq ? "failed" : "ok");
                    if (neq) {
                        failed = true;
                    }
                } else if (answer->m_types[col] == voltdb::ValueType::tINTEGER) {
                    int32_t v1 = voltdb::ValuePeeker::peekAsInteger(tuple.getNValue(col));
                    VOLT_TRACE("Row %02d, col %02d: expected %04d, got %04d (%s)",
                               row, col,
                               expected, v1,
                               (expected != v1) ? "failed" : "ok");
                    if (expected != v1) {
                        failed = true;
                    }
                } else {
                    std::ostringstream oss;
                    oss << "Value type "
                        << getTypeName(answer->m_types[col])
                        << " Only "
                        << getTypeName(voltdb::ValueType::tINTEGER)
                        << " and "
                        << getTypeName(voltdb::ValueType::tVARCHAR)
                        << " are supported."
                        << std::endl;
                    throw std::logic_error(oss.str());
                }
            }
        }
        bool hasNext = iter.next(tuple);
        if (hasNext) {
            throw std::logic_error("Unexpected next element\n");
        }
        ASSERT_EQ(expectFail, failed);
    }

    void validateDMLResultTable(voltdb::TempTable *result, int64_t expectedModifiedTuples = 1) {
        ASSERT_TRUE(result);
        const voltdb::TupleSchema* resultSchema = result->schema();
        voltdb::TableTuple tuple(resultSchema);
        voltdb::TableIterator iter = result->iterator();
        ASSERT_TRUE(iter.next(tuple));
        int64_t actualModifiedTuples = voltdb::ValuePeeker::peekBigInt(tuple.getNValue(0));
        ASSERT_EQ(expectedModifiedTuples, actualModifiedTuples);
        ASSERT_FALSE(iter.next(tuple));
    }

    void initParamsBuffer() {
        m_paramsOutput.initializeWithPosition(m_parameter_buffer.get(),
                                              m_smallBufferSize,
                                              0);
    }

    void prepareParamsBufferForNextFragment() {
        m_paramCount = 0;
        m_paramCountOffset = m_paramsOutput.reserveBytes(sizeof(int16_t));
        m_paramsOutput.writeShortAt(m_paramCountOffset, m_paramCount);
    }

    void addParameterToBuffer(voltdb::ValueType type, const void *buf, int32_t length = OBJECTLENGTH_NULL) {
        m_paramsOutput.writeByte(static_cast<int8_t>(type));
        switch (type) {
            case voltdb::ValueType::tVARCHAR:
            case voltdb::ValueType::tVARBINARY:
                if (buf == NULL) {
                    m_paramsOutput.writeInt(OBJECTLENGTH_NULL);
                    break;
                }
                if (length <= OBJECTLENGTH_NULL) {
                    // Attempted to serialize a value with a negative length
                    ASSERT_TRUE(false);
                }
                m_paramsOutput.writeInt(length);
                m_paramsOutput.writeBytes(buf, length);
                break;
            case voltdb::ValueType::tTINYINT:
                m_paramsOutput.writeByte(*static_cast<const int8_t*>(buf));
                break;
            case voltdb::ValueType::tSMALLINT:
                m_paramsOutput.writeShort(*static_cast<const int16_t*>(buf));
                break;
            case voltdb::ValueType::tINTEGER:
                m_paramsOutput.writeInt(*static_cast<const int32_t*>(buf));
                break;
            case voltdb::ValueType::tTIMESTAMP:
                m_paramsOutput.writeLong(*static_cast<const int64_t*>(buf));
                break;
            case voltdb::ValueType::tBIGINT:
                m_paramsOutput.writeLong(*static_cast<const int64_t*>(buf));
                break;
            case voltdb::ValueType::tDOUBLE:
                m_paramsOutput.writeDouble(*static_cast<const double*>(buf));
                break;
            default:
                // Unsupported type.
                ASSERT_TRUE(false);
        }
        m_paramCount++;
        m_paramsOutput.writeShortAt(m_paramCountOffset, m_paramCount);
    }

protected:
    voltdb::CatalogId m_cluster_id;
    voltdb::CatalogId m_database_id;
    voltdb::CatalogId m_site_id;
    std::string m_catalog_string;
    // This is not the real catalog that the VoltDBEngine uses.
    // It is a duplicate made locally to get GUIDs
    catalog::Catalog     *m_catalog;
    catalog::Cluster *m_cluster;
    catalog::Database *m_database;
    catalog::Constraint *m_constraint;
    boost::scoped_ptr<voltdb::VoltDBEngine>     m_engine;
    boost::scoped_ptr<TOPEND> m_topend;
    boost::shared_array<char>m_result_buffer;
    boost::shared_array<char>m_exception_buffer;
    boost::shared_array<char>m_parameter_buffer;
    boost::shared_array<char>                m_per_fragment_stats_buffer;
    bool                     m_isinitialized;
    int                      m_fragmentNumber;
    size_t                                   m_paramCountOffset;
    int16_t                                  m_paramCount;
    voltdb::ReferenceSerializeOutput         m_paramsOutput;
    // The size for all the synthetic buffers except the result buffer.
    static const size_t  m_smallBufferSize = 4 * 1024;
    // The size of the result buffer.
    static const size_t m_resultBufferSize = 1024 * 1024 * 2;
    voltdb::Pool        m_pool;
};

