/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

#include "harness.h"
#include "topics/encode/Encoder.h"

using namespace voltdb;
using namespace voltdb::topics;

class EncoderTestBase : public Test {
public:
    virtual ~EncoderTestBase() {
        TupleSchema::freeTupleSchema(m_schema);
    }

protected:
    void validateHeader(SerializeInputBE& in, int32_t schemaId) {
        ASSERT_EQ(0, in.readByte()); // Magic value
        ASSERT_EQ(schemaId, in.readInt()); // schema ID
    }

    void setupAllSchema(bool nullable) {
        std::vector<ValueType> types { ValueType::tTINYINT, ValueType::tSMALLINT, ValueType::tINTEGER,
                        ValueType::tBIGINT, ValueType::tDOUBLE, ValueType::tTIMESTAMP, ValueType::tDECIMAL, ValueType::tVARCHAR,
                        ValueType::tVARBINARY, ValueType::tPOINT, ValueType::tGEOGRAPHY };
        std::vector<int32_t> sizes { 1, 2, 4, 8, 8, 8, 16, 256, 256, 16, 1024 };
        std::vector<bool> nullables(11, nullable);
        m_schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullables);
        setupTuple();
    }

    void setupTuple() {
        m_tuple = TableTuple(m_schema);
        m_tupleData.reset(new char[m_tuple.tupleLength()]);
        m_tuple.moveAndInitialize(m_tupleData.get());
    }

    void insertValues(int8_t tinyint, int16_t smallint, int32_t integer, int64_t bigint, double dbl, int64_t timestamp,
            double decimal, const std::string& varChar, const std::vector<uint8_t>& varBinary,
            const GeographyPointValue* point, const Polygon* geography) {
        m_tuple.setNValue(0, ValueFactory::getTinyIntValue(tinyint));
        m_tuple.setNValue(1, ValueFactory::getSmallIntValue(smallint));
        m_tuple.setNValue(2, ValueFactory::getIntegerValue(integer));
        m_tuple.setNValue(3, ValueFactory::getBigIntValue(bigint));
        m_tuple.setNValue(4, ValueFactory::getDoubleValue(dbl));
        m_tuple.setNValue(5, ValueFactory::getTimestampValue(timestamp));
        m_tuple.setNValue(6, ValueFactory::getDecimalValue(decimal));
        m_tuple.setNValue(7, ValueFactory::getStringValue(varChar, &m_pool));
        m_tuple.setNValue(8, ValueFactory::getBinaryValue(varBinary.data(), varBinary.size(), &m_pool));
        m_tuple.setNValue(9, ValueFactory::getGeographyPointValue(point));
        m_tuple.setNValue(10, ValueFactory::getGeographyValue(geography, &m_pool));
    }

    TupleSchema *m_schema;
    Pool m_pool;
    std::unique_ptr<char[]> m_tupleData;
    TableTuple m_tuple;
};


