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

#include "harness.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "test_utils/plan_testing_baseclass.h"
#include "PerFragmentStatsTest.hpp"

class PerFragmentStatsTest : public PlanTestingBaseClass<EngineTestTopend> {
public:
    PerFragmentStatsTest() :
        PlanTestingBaseClass<EngineTestTopend>(),
        m_tableT(NULL),
        m_tableT_id(-1) {}

    void initialize(const char *catalog_string) {
        PlanTestingBaseClass<EngineTestTopend>::initialize(catalog_string);
        m_tableT = getPersistentTableAndId("T", &m_tableT_id, NULL);
        ASSERT_TRUE(m_tableT);
    }

protected:
    void addParameters(int32_t valueA, double valueB, std::string valueC) {
        prepareParamsBufferForNextFragment();
        addParameterToBuffer(voltdb::ValueType::tINTEGER, &valueA);
        addParameterToBuffer(voltdb::ValueType::tDOUBLE,  &valueB);
        addParameterToBuffer(voltdb::ValueType::tVARCHAR, valueC.c_str(), valueC.size());
    }

    void validateRow(voltdb::TableTuple &tuple, int32_t valueA, double valueB, const char* valueC) {
        ASSERT_EQ(valueA, voltdb::ValuePeeker::peekInteger(tuple.getNValue(0)));
        ASSERT_EQ(valueB, voltdb::ValuePeeker::peekDouble(tuple.getNValue(1)));

        int32_t length = 0;
        const char* data = voltdb::ValuePeeker::peekObject(tuple.getNValue(2), &length);
        ASSERT_EQ(0, strncmp(valueC, data, length));
    }

    void validatePerFragmentStatsBuffer(int32_t expectedSucceededFragmentsCount, int32_t batchSize) {
        voltdb::ReferenceSerializeInputBE perFragmentStatsBuffer(m_per_fragment_stats_buffer.get(), m_smallBufferSize);
        // Skip the perFragmentTimingEnabled flag.
        perFragmentStatsBuffer.readByte();
        int32_t actualSucceededFragmentsCount = perFragmentStatsBuffer.readInt();
        ASSERT_EQ(expectedSucceededFragmentsCount, actualSucceededFragmentsCount);
        int32_t numOfValuesToCheck = expectedSucceededFragmentsCount;
        // If the batch failed in the middle, the time measurement for the failed fragment also
        // needs to be validated.
        if (batchSize > expectedSucceededFragmentsCount) {
            numOfValuesToCheck++;
        }
        for (int32_t i = 0; i < numOfValuesToCheck; i++) {
            int64_t elapsedNanoseconds = perFragmentStatsBuffer.readLong();
            ASSERT_GT(elapsedNanoseconds, 0);
        }
    }

    voltdb::PersistentTable* m_tableT;
    int m_tableT_id;
};

TEST_F(PerFragmentStatsTest, TestPerFragmentStatsBuffer) {
    // catalogPayload, anInsertPlan, and aSelectPlan are defined in PerFragmentStatsTest.hpp
    initialize(catalogPayload);
    // Set perFragmentTimingEnabled bit to true, all the fragments will be timed.
    voltdb::ReferenceSerializeOutput perFragmentStatsOutput;
    perFragmentStatsOutput.initializeWithPosition(m_per_fragment_stats_buffer.get(), m_smallBufferSize, 0);
    perFragmentStatsOutput.writeByte(static_cast<int8_t>(1));
    // Add query plans.
    fragmentId_t insertPlanId = 100;
    fragmentId_t selectPlanId = 200;
    m_topend->addPlan(insertPlanId, anInsertPlan);
    m_topend->addPlan(selectPlanId, aSelectPlan);
    // First, build a query batch that can run successfully:
    fragmentId_t* planfragmentIds = new fragmentId_t[4];
    planfragmentIds[0] = insertPlanId; planfragmentIds[1] = insertPlanId;
    planfragmentIds[2] = insertPlanId; planfragmentIds[3] = selectPlanId;
    // Prepare parameters for each fragment (statement):
    initParamsBuffer();
    // Fragment #1: INSERT INTO T VALUES (1, 2.3, 'string');
    addParameters(1, 2.3, "string");
    // Fragment #2: INSERT INTO T VALUES (1, 4.5, 'string');
    addParameters(1, 4.5, "string");
    // Fragment #3: INSERT INTO T VALUES (1, 6.7, 'string');
    addParameters(1, 6.7, "string");
    // Fragment #4: SELECT * FROM T WHERE a = 1 and b >= 4.0 and C like 'str%';
    addParameters(1, 4.0, "str%%");
    voltdb::ReferenceSerializeInputBE params(m_parameter_buffer.get(), m_smallBufferSize);
    m_engine->resetPerFragmentStatsOutputBuffer();
    // This batch should succeed and return 0.
    ASSERT_EQ(0, m_engine->executePlanFragments(4, planfragmentIds, NULL, params, 1000, 1000, 1000, 1000, 1, false));
    // Fetch the results. We have forced them to be written
    // to our own buffer in the local engine.  But we don't
    // know how much of the buffer is actually used.  So we
    // need to query the engine.
    size_t resultSize = m_engine->getResultsSize();
    voltdb::ReferenceSerializeInputBE resultBuffer(m_result_buffer.get(), resultSize);
    boost::scoped_ptr<voltdb::TempTable> result(NULL);
    // Validate the result of the first 3 DMLs.
    for (int i = 0; i < 3; i++) {
        // "i > 0" is the boolean flag for determining whether to skip the message header.
        result.reset(voltdb::loadTableFrom(resultBuffer, i > 0));
        validateDMLResultTable(result.get());
    }
    // Validate the result of the last SELECT statement.
    result.reset(voltdb::loadTableFrom(resultBuffer, true));
    ASSERT_TRUE(result);
    const voltdb::TupleSchema* resultSchema = result->schema();
    voltdb::TableTuple tuple(resultSchema);
    voltdb::TableIterator iter = result->iterator();
    ASSERT_TRUE(iter.next(tuple));
    validateRow(tuple, 1, 4.5, "string");
    ASSERT_TRUE(iter.next(tuple));
    validateRow(tuple, 1, 6.7, "string");
    ASSERT_FALSE(iter.next(tuple));
    // Validate the content in the per fragment statistics buffer.
    validatePerFragmentStatsBuffer(4, 4); // 4 out of 4 fragments succeeded.

    // Now, let the third fragment fail the batch.
    // Fragment #1: INSERT INTO T VALUES (1, 8.9, 'string');
    addParameters(1, 8.9, "string");
    // Fragment #2: INSERT INTO T VALUES (1, 10.11, 'string');
    addParameters(1, 10.11, "string");
    // Fragment #3: INSERT INTO T VALUES (1, 12.13, 'string that exceeds the limit');
    addParameters(1, 12.13, "string that exceeds the limit");
    // Fragment #4: SELECT * FROM T WHERE a = 1 and b >= 4.0 and C like 'str%';
    addParameters(1, 4.0, "str%%");
    // This batch should FAIL and return 1.
    m_engine->resetPerFragmentStatsOutputBuffer();
    ASSERT_EQ(1, m_engine->executePlanFragments(4, planfragmentIds, NULL, params, 1001, 1001, 1001, 1001, 2, false));
    // Verify that 2 out of 4 fragments succeeded.
    validatePerFragmentStatsBuffer(2, 4);
    delete[] planfragmentIds;
}

int main() {
     return TestSuite::globalInstance()->runAll();
}
