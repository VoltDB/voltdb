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
 */

#ifndef TESTS_EE_TEST_UTILS_PLAN_TESTING_BASECLASS_H_
#define TESTS_EE_TEST_UTILS_PLAN_TESTING_BASECLASS_H_

#include "catalog/cluster.h"
#include "catalog/constraint.h"
#include "catalog/table.h"
#include "common/SerializableEEException.h"
#include "common/ValueFactory.hpp"
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
        m_parameter_buffer.reset(new char [m_bufferSize]);
        m_granular_stats_buffer.reset(new char [m_bufferSize]);
        m_result_buffer.reset(new char [1024 * 1024 * 2]);
        m_exception_buffer.reset(new char [m_bufferSize]);
        m_engine->setBuffers(m_parameter_buffer.get(), m_bufferSize,
                             m_granular_stats_buffer.get(), m_bufferSize,
                             m_result_buffer.get(), 1024 * 1024 * 2,
                             m_exception_buffer.get(), m_bufferSize);
        m_engine->resetReusedResultOutputBuffer();
        m_engine->resetGranularStatisticsOutputBuffer();
        int partitionCount = 3;
        m_engine->initialize(m_cluster_id, m_site_id, 0, 0, "", 0, 1024, voltdb::DEFAULT_TEMP_TABLE_MEMORY, false);
        m_engine->updateHashinator(voltdb::HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);
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
            //
        }

    voltdb::PersistentTable *getPersistentTableAndId(const std::string &name, int *id) {
        catalog::Table *tbl = m_database->tables().get(name);
        assert(tbl != NULL);
        if (id != NULL) {
            *id = tbl->relativeIndex();
        }
        return dynamic_cast<voltdb::PersistentTable *>(m_engine->getTableByName(name));
    }

    void initTable(voltdb::PersistentTable       **table,
                   int                            *table_id,
                   const TableConfig              *oneTable) {
        initializeTableOfInt(oneTable->m_tableName,
                             table,
                             table_id,
                             oneTable->m_numRows,
                             oneTable->m_numCols,
                             oneTable->m_contents);
    }

    void initTable(const TableConfig *oneTable) {
        initTable(NULL, NULL, oneTable);
    }

    void initializeTableOfInt(std::string                     tableName,
                              voltdb::PersistentTable       **table,
                              int                            *tableId,
                              int                             nRows,
                              int                             nCols,
                              const int32_t                  *vals) {
        voltdb::PersistentTable *pTable = getPersistentTableAndId(tableName.c_str(), tableId);
        assert(pTable != NULL);
        if (table != NULL) {
            *table = pTable;
        }
        for (int row = 0; row < nRows; row += 1) {
            voltdb::TableTuple &tuple = pTable->tempTuple();
            for (int col = 0; col < nCols; col += 1) {
                int32_t val = vals[(row*nCols) + col];
                voltdb::NValue nval = voltdb::ValueFactory::getIntegerValue(val);
                tuple.setNValue(col, nval);
            }
            if (!pTable->insertTuple(tuple)) {
                return;
            }
        }
    }

    /**
     * Execute a single test.  Execute the test's fragment, and then
     * validate the output table.
     */
    void executeTest(const TestConfig &test) {
        // The fragment number doesn't really matter here.
        executeFragment(m_fragmentNumber, test.m_planString);
        validateResult((const int32_t *)test.m_outputTable, test.m_numOutputRows, test.m_numOutputCols);
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
            memset(m_parameter_buffer.get(), 0, m_bufferSize);
            voltdb::ReferenceSerializeInputBE emptyParams(m_parameter_buffer.get(), m_bufferSize);

            //
            // Execute the plan.  You'd think this would be more
            // impressive.
            //
            try {
                m_engine->executePlanFragments(1, &fragmentId, NULL, emptyParams, 1000, 1000, 1000, 1000, 1);
            } catch (voltdb::SerializableEEException &ex) {
                throw;
            }
    }

    /**
     * Fetch the results.  We have forced them to be written
     *
     * to our own buffer in the local engine.  But we don't
     * know how much of the buffer is actually used.  So we
     * need to query the engine.
     */
    void validateResult(const int *answer, int nRows, int nCols) {
        size_t result_size = m_engine->getResultsSize();
        boost::scoped_ptr<voltdb::TempTable> result(voltdb::loadTableFrom(m_result_buffer.get(), result_size));
        assert(result.get() != NULL);
        ASSERT_TRUE(result != NULL);

        const voltdb::TupleSchema* res_schema = result->schema();
        voltdb::TableTuple tuple(res_schema);
        voltdb::TableIterator &iter = result->iterator();
        if (!iter.hasNext() && nRows > 0) {
            printf("No results!!\n");
            ASSERT_FALSE(true);
        }
        ASSERT_EQ(nCols, result->columnCount());
        bool failed = false;
        for (int32_t row = 0; row < nRows; row += 1) {
            ASSERT_TRUE(iter.next(tuple));
            for (int32_t col = 0; col < nCols; col += 1) {
                int32_t expected = answer[row * nCols + col];
                int64_t v1 = voltdb::ValuePeeker::peekAsBigInt(tuple.getNValue(col));
                VOLT_TRACE("Row %02d, col %02d: expected %04d, got %04ld (%s)",
                           row, col,
                           expected, v1,
                           (expected != v1) ? "failed" : "ok");
                if (expected != v1) {
                    failed = true;
                }
            }
        }
        bool hasNext = iter.next(tuple);
        if (hasNext) {
            VOLT_TRACE("Unexpected next element\n");
            failed = true;
        }
        ASSERT_FALSE(failed);
    }

    void validateDMLResultTable(voltdb::TempTable *result, int64_t expectedModifiedTuples = 1) {
        ASSERT_TRUE(result);
        const voltdb::TupleSchema* resultSchema = result->schema();
        voltdb::TableTuple tuple(resultSchema);
        boost::scoped_ptr<voltdb::TableIterator> iter(result->makeIterator());
        ASSERT_TRUE(iter->next(tuple));
        int64_t actualModifiedTuples = voltdb::ValuePeeker::peekBigInt(tuple.getNValue(0));
        ASSERT_EQ(expectedModifiedTuples, actualModifiedTuples);
        ASSERT_FALSE(iter->next(tuple));
    }

    void initParamsBuffer() {
        m_paramsOutput.initializeWithPosition(m_parameter_buffer.get(),
                                              m_bufferSize,
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
            case voltdb::VALUE_TYPE_VARCHAR:
            case voltdb::VALUE_TYPE_VARBINARY:
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
            case voltdb::VALUE_TYPE_TINYINT:
                m_paramsOutput.writeByte(*static_cast<const int8_t*>(buf));
                break;
            case voltdb::VALUE_TYPE_SMALLINT:
                m_paramsOutput.writeShort(*static_cast<const int16_t*>(buf));
                break;
            case voltdb::VALUE_TYPE_INTEGER:
                m_paramsOutput.writeInt(*static_cast<const int32_t*>(buf));
                break;
            case voltdb::VALUE_TYPE_TIMESTAMP:
                m_paramsOutput.writeLong(*static_cast<const int64_t*>(buf));
                break;
            case voltdb::VALUE_TYPE_BIGINT:
                m_paramsOutput.writeLong(*static_cast<const int64_t*>(buf));
                break;
            case voltdb::VALUE_TYPE_DOUBLE:
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
    voltdb::CatalogId    m_cluster_id;
    voltdb::CatalogId    m_database_id;
    voltdb::CatalogId    m_site_id;
    std::string          m_catalog_string;
    // This is not the real catalog that the VoltDBEngine uses.
    // It is a duplicate made locally to get GUIDs
    catalog::Catalog     *m_catalog;
    catalog::Cluster     *m_cluster;
    catalog::Database    *m_database;
    catalog::Constraint  *m_constraint;
    boost::scoped_ptr<voltdb::VoltDBEngine>  m_engine;
    boost::scoped_ptr<TOPEND>                m_topend;
    boost::shared_array<char>                m_result_buffer;
    boost::shared_array<char>                m_exception_buffer;
    boost::shared_array<char>                m_parameter_buffer;
    boost::shared_array<char>                m_granular_stats_buffer;
    bool                                     m_isinitialized;
    int                                      m_fragmentNumber;
    size_t                                   m_paramCountOffset;
    int16_t                                  m_paramCount;
    voltdb::ReferenceSerializeOutput         m_paramsOutput;

    static const size_t       m_bufferSize = 4 * 1024;
};

#endif /* TESTS_EE_TEST_UTILS_PLAN_TESTING_BASECLASS_H_ */
