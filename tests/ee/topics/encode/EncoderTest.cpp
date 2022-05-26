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

TEST_F(Test, NullEncoder) {
    TableTuple tuple;
    ReferenceSerializeOutput eso(nullptr, 0);

    NullEncoder ne;
    ASSERT_EQ(-1, ne.sizeOf(tuple));
    ASSERT_EQ(-1, ne.encode(eso, tuple));
    ASSERT_EQ(0, eso.position());
}

TEST_F(Test, IntEncoder) {
    std::vector<ValueType> types(3, ValueType::tINTEGER);
    std::vector<int32_t> sizes(3, 4);
    std::vector<bool> nullables { false, false, true };
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullables);
    TableTuple tuple(schema);
    std::unique_ptr<char[]> tupleData(new char[tuple.tupleLength()]);
    tuple.moveAndInitialize(tupleData.get());

    int32_t val1 = 8489743, val2 = -84343;
    char data[sizeof(val1)];
    tuple.setNValue(0, ValueFactory::getIntegerValue(val1));
    tuple.setNValue(1, ValueFactory::getIntegerValue(val2));
    tuple.setNValue(2, NValue::getNullValue(ValueType::tINTEGER));

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<IntEncoder> encoder(0);
        ASSERT_EQ(sizeof(val1), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val1), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val1), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        ASSERT_EQ(val1, in.readInt());
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<IntEncoder> encoder(1);
        ASSERT_EQ(sizeof(val2), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val2), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val2), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        ASSERT_EQ(val2, in.readInt());
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<IntEncoder> encoder(2);
        ASSERT_EQ(-1, encoder.sizeOf(tuple));
        ASSERT_EQ(-1, encoder.encode(out, tuple));
        ASSERT_EQ(0, out.position());
    }

    TupleSchema::freeTupleSchema(schema);
}

TEST_F(Test, BigIntEncoder) {
    std::vector<ValueType> types(3, ValueType::tBIGINT);
    std::vector<int32_t> sizes(3, 8);
    std::vector<bool> nullables { false, false, true };
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullables);
    TableTuple tuple(schema);
    std::unique_ptr<char[]> tupleData(new char[tuple.tupleLength()]);
    tuple.moveAndInitialize(tupleData.get());


    int64_t val1 = 8489743894735, val2 = -84343894981;
    char data[sizeof(val1)];

    tuple.setNValue(0, ValueFactory::getBigIntValue(val1));
    tuple.setNValue(1, ValueFactory::getBigIntValue(val2));
    tuple.setNValue(2, NValue::getNullValue(ValueType::tBIGINT));

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<BigIntEncoder> encoder(0);
        ASSERT_EQ(sizeof(val1), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val1), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val1), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        ASSERT_EQ(val1, in.readLong());
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<BigIntEncoder> encoder(1);
        ASSERT_EQ(sizeof(val2), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val2), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val2), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        ASSERT_EQ(val2, in.readLong());
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<BigIntEncoder> encoder(2);
        ASSERT_EQ(-1, encoder.sizeOf(tuple));
        ASSERT_EQ(-1, encoder.encode(out, tuple));
        ASSERT_EQ(0, out.position());
    }

    TupleSchema::freeTupleSchema(schema);
}


TEST_F(Test, DoubleEncoder) {
    std::vector<ValueType> types(3, ValueType::tDOUBLE);
    std::vector<int32_t> sizes(3, 8);
    std::vector<bool> nullables { false, false, true };
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullables);
    TableTuple tuple(schema);
    std::unique_ptr<char[]> tupleData(new char [tuple.tupleLength()]);
    tuple.moveAndInitialize(tupleData.get());


    double val1 = 8489.743894735, val2 = -843438949.81;
    char data[sizeof(val1)];

    tuple.setNValue(0, ValueFactory::getDoubleValue(val1));
    tuple.setNValue(1, ValueFactory::getDoubleValue(val2));
    tuple.setNValue(2, NValue::getNullValue(ValueType::tDOUBLE));

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<DoubleEncoder> encoder(0);
        ASSERT_EQ(sizeof(val1), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val1), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val1), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        ASSERT_EQ(val1, in.readDouble());
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<DoubleEncoder> encoder(1);
        ASSERT_EQ(sizeof(val2), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val2), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val2), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        ASSERT_EQ(val2, in.readDouble());
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<DoubleEncoder> encoder(2);
        ASSERT_EQ(-1, encoder.sizeOf(tuple));
        ASSERT_EQ(-1, encoder.encode(out, tuple));
        ASSERT_EQ(0, out.position());
    }

    TupleSchema::freeTupleSchema(schema);
}

TEST_F(Test, VarCharEncoder) {
    std::vector<ValueType> types(3, ValueType::tVARCHAR);
    std::vector<int32_t> sizes(3, 256);
    std::vector<bool> nullables { false, false, true };
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullables);
    TableTuple tuple(schema);
    std::unique_ptr<char[]> tupleData(new char[tuple.tupleLength()]);
    tuple.moveAndInitialize(tupleData.get());


    const char *val1 = "some string to test", *val2 = "another different string";
    char data[256];

    Pool pool;
    tuple.setNValue(0, ValueFactory::getStringValue(val1, &pool));
    tuple.setNValue(1, ValueFactory::getStringValue(val2, &pool));
    tuple.setNValue(2, NValue::getNullValue(ValueType::tDOUBLE));

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<PlainVarLenEncoder> encoder(0);
        ASSERT_EQ(strlen(val1), encoder.sizeOf(tuple));
        ASSERT_EQ(strlen(val1), encoder.encode(out, tuple));
        ASSERT_EQ(strlen(val1), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        char decoded[strlen(val1)];
        in.readBytes(decoded, strlen(val1));
        ASSERT_EQ(val1, std::string(decoded, strlen(val1)));
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<PlainVarLenEncoder> encoder(1);
        ASSERT_EQ(strlen(val2), encoder.sizeOf(tuple));
        ASSERT_EQ(strlen(val2), encoder.encode(out, tuple));
        ASSERT_EQ(strlen(val2), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        char decoded[strlen(val2)];
        in.readBytes(decoded, strlen(val2));
        ASSERT_EQ(val2, std::string(decoded, strlen(val2)));
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<PlainVarLenEncoder> encoder(2);
        ASSERT_EQ(-1, encoder.sizeOf(tuple));
        ASSERT_EQ(-1, encoder.encode(out, tuple));
        ASSERT_EQ(0, out.position());
    }

    TupleSchema::freeTupleSchema(schema);
}

TEST_F(Test, VarBinaryEncoder) {
    std::vector<ValueType> types(3, ValueType::tVARBINARY);
    std::vector<int32_t> sizes(3, 256);
    std::vector<bool> nullables { false, false, true };
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullables);
    TableTuple tuple(schema);
    std::unique_ptr<char[]> tupleData(new char[tuple.tupleLength()]);
    tuple.moveAndInitialize(tupleData.get());


    char unsigned val1[] = {'a', 'b', 'X', '5', 3, 120, 89},
            val2[] = {5, 189, 74, 15, 69, 0, 78, 90, 78, 124};
    char data[256];

    Pool pool;
    tuple.setNValue(0, ValueFactory::getBinaryValue(val1, sizeof(val1), &pool));
    tuple.setNValue(1, ValueFactory::getBinaryValue(val2, sizeof(val2), &pool));
    tuple.setNValue(2, NValue::getNullValue(ValueType::tDOUBLE));

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<PlainVarLenEncoder> encoder(0);
        ASSERT_EQ(sizeof(val1), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val1), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val1), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        unsigned char decoded[sizeof(val1)];
        in.readBytes(decoded, sizeof(val1));
        ASSERT_EQ(0, ::memcmp(val1, decoded, sizeof(val1)));
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<PlainVarLenEncoder> encoder(1);
        ASSERT_EQ(sizeof(val2), encoder.sizeOf(tuple));
        ASSERT_EQ(sizeof(val2), encoder.encode(out, tuple));
        ASSERT_EQ(sizeof(val2), out.position());

        ReferenceSerializeInputBE in(data, sizeof(data));

        unsigned char decoded[sizeof(val2)];
        in.readBytes(decoded, sizeof(val2));
        ASSERT_EQ(0, ::memcmp(val2, decoded, sizeof(val2)));
    }

    {
        ReferenceSerializeOutput out(&data, sizeof(data));
        SingleValueEncoder<PlainVarLenEncoder> encoder(2);
        ASSERT_EQ(-1, encoder.sizeOf(tuple));
        ASSERT_EQ(-1, encoder.encode(out, tuple));
        ASSERT_EQ(0, out.position());
    }

    TupleSchema::freeTupleSchema(schema);
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
