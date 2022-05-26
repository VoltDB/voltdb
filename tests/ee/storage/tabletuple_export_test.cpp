/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/ValueFactory.hpp"
#include "common/serializeio.h"
#include "common/ExportSerializeIo.h"
#include "common/ThreadLocalPool.h"

#include <cstdlib>

using namespace voltdb;

class TableTupleExportTest : public Test {

  protected:
    // some utility functions to verify
    size_t maxElSize(std::vector<uint16_t> &keep_offsets, bool useNullStrings=false);
    size_t serElSize(std::vector<uint16_t> &keep_offsets, uint8_t*, char*, bool nulls=false);
    void verSer(int, char*);
    ThreadLocalPool m_pool;
  public:
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull;
    TupleSchema *m_schema;

    void addToSchema(ValueType vt, bool allownull) {
        columnTypes.push_back(vt);
        columnLengths.push_back(NValue::getTupleStorageSize(vt));
        columnAllowNull.push_back(allownull);
    }

    TableTupleExportTest() {
        bool allownull = true;

        // note that maxELSize() cares about the string tuple offsets..

        // set up a schema with each supported column type
        addToSchema(ValueType::tTINYINT, allownull);  // 0
        addToSchema(ValueType::tSMALLINT, allownull); // 1
        addToSchema(ValueType::tINTEGER, allownull);  // 2
        addToSchema(ValueType::tBIGINT, allownull);   // 3
        addToSchema(ValueType::tTIMESTAMP, allownull); // 4
        addToSchema(ValueType::tDECIMAL, allownull);   // 5

        // need explicit lengths for varchar columns
        columnTypes.push_back(ValueType::tVARCHAR);  // 6
        columnLengths.push_back(15);
        columnAllowNull.push_back(allownull);

        columnTypes.push_back(ValueType::tVARCHAR);   // 7
        columnLengths.push_back(UNINLINEABLE_OBJECT_LENGTH * 2);
        columnAllowNull.push_back(allownull);

        m_schema = TupleSchema::createTupleSchemaForTest(
            columnTypes, columnLengths, columnAllowNull);
    }

    ~TableTupleExportTest() {
        TupleSchema::freeTupleSchema(m_schema);
    }

};


// helper to make a schema, a tuple and calculate EL size
size_t
TableTupleExportTest::maxElSize(std::vector<uint16_t> &keep_offsets,
                             bool useNullStrings)
{
    TableTuple *tt;
    TupleSchema *ts;
    char buf[1024]; // tuple data
    buf[0] = 0x0; // set tuple header to defaults

    ts = TupleSchema::createTupleSchema(m_schema, keep_offsets);
    tt = new TableTuple(buf, ts);

    // if the tuple includes strings, add some content
    // assuming all Export tuples were allocated for persistent
    // storage and choosing set* api accordingly here.
    if (ts->columnCount() > 6) {
        NValue nv = ValueFactory::getStringValue("ABCDEabcde"); // 10 char
        if (useNullStrings)
        {
            nv.free(); nv.setNull();
        }
        tt->setNValueAllocateForObjectCopies(6, nv);
        nv.free();
    }
    if (ts->columnCount() > 7) {
        NValue nv = ValueFactory::getStringValue("abcdeabcdeabcdeabcde"); // 20 char
        if (useNullStrings)
        {
            nv.free(); nv.setNull();
        }
        tt->setNValueAllocateForObjectCopies(7, nv);
        nv.free();
    }

    // The function under test!
    size_t sz = tt->maxExportSerializationSize();

    // and cleanup
    tt->freeObjectColumns();
    delete tt;
    TupleSchema::freeTupleSchema(ts);

    return sz;
}

/*
 * Verify that the max tuple size returns expected result
 */
TEST_F(TableTupleExportTest, maxExportSerSize_tiny) {

    // create a schema by selecting a column from the super-set.
    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // just tinyint in schema
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(1, sz);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(3, sz);

    // + integer
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(7, sz);

    // + bigint
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(15, sz);

    // + timestamp
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(23, sz);

    // + decimal
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(41, sz);

    // + first varchar
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(55, sz); // length, 10 chars

    // + second varchar
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(79, sz); // length, 20 chars
}

/*
 * Verify that the max tuple size returns expected result using null strings
 */
TEST_F(TableTupleExportTest, maxExportSerSize_withNulls) {

    // create a schema by selecting a column from the super-set.
    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // just tinyint in schema
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(1, sz);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(3, sz);

    // + integer
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(7, sz);

    // + bigint
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(15, sz);

    // + timestamp
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(23, sz);

    // + decimal
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets);
    EXPECT_EQ(41, sz);

    // + first varchar
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets, true);
    EXPECT_EQ(41, sz);

    // + second varchar
    keep_offsets.push_back(i++);
    sz = maxElSize(keep_offsets, true);
    EXPECT_EQ(41, sz);
}

// helper to make a schema, a tuple and serialize to a buffer
size_t
TableTupleExportTest::serElSize(std::vector<uint16_t> &keep_offsets,
                             uint8_t *nullArray, char *dataPtr, bool nulls)
{
    TableTuple *tt;
    TupleSchema *ts;
    char buf[1024]; // tuple data
    buf[0] = 0x0; // set tuple header to defaults

    ts = TupleSchema::createTupleSchema(m_schema, keep_offsets);
    tt = new TableTuple(buf, ts);

    // assuming all Export tuples were allocated for persistent
    // storage and choosing set* api accordingly here.

    switch (ts->columnCount()) {
        // note my sophisticated and clever use of fall through
      case 8:
      {
          NValue nv = ValueFactory::getStringValue("abcdeabcdeabcdeabcde"); // 20 char
          if (nulls) { nv.free(); nv.setNull(); }
          tt->setNValueAllocateForObjectCopies(7, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 7:
      {
          NValue nv = ValueFactory::getStringValue("ABCDEabcde"); // 10 char
          if (nulls) { nv.free(); nv.setNull(); }
          tt->setNValueAllocateForObjectCopies(6, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 6:
      {
          NValue nv = ValueFactory::getDecimalValueFromString("-12.34");
          if (nulls) { nv.free(); nv.setNull(); }
          tt->setNValueAllocateForObjectCopies(5, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 5:
      {
          NValue nv = ValueFactory::getTimestampValue(9999);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(4, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 4:
      {
          NValue nv = ValueFactory::getBigIntValue(1024);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(3, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 3:
      {
          NValue nv = ValueFactory::getIntegerValue(512);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(2, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 2:
      {
          NValue nv = ValueFactory::getSmallIntValue(256);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(1, nv);
          nv.free();
      }
      /* fall through */ // gcc-7 needs this comment.
      case 1:
      {
          NValue nv = ValueFactory::getTinyIntValue(120);
          if (nulls) nv.setNull();
          tt->setNValueAllocateForObjectCopies(0, nv);
          nv.free();
      }
      break;

      default:
        // this is an error in the test fixture.
        EXPECT_EQ(0,1);
        break;
    }

    // The function under test!
    ExportSerializeOutput io(dataPtr, 2048);
    tt->serializeToExport(io, 0, nullArray);

    // and cleanup
    tt->freeObjectColumns();
    delete tt;
    TupleSchema::freeTupleSchema(ts);
    return io.position();
}

// helper to verify the data that was serialized to the buffer
void
TableTupleExportTest::verSer(int cnt, char *data)
{
    ExportSerializeInput sin(data, 2048);

    if (cnt-- >= 0)
    {
        int8_t v = sin.readByte();
        EXPECT_EQ(120, v);
    }
    if (cnt-- >= 0)
    {
        int16_t v = sin.readShort();
        EXPECT_EQ(256, v);
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(512, sin.readInt());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(1024, sin.readLong());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(9999, sin.readLong());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(12, sin.readByte());
        EXPECT_EQ(16, sin.readByte());
        int64_t low = sin.readLong();
        low = ntohll(low);
        int64_t high = sin.readLong();
        high = ntohll(high);
        NValue nv = ValueFactory::getDecimalValueFromString("-12.34");
        TTInt val = ValuePeeker::peekDecimal(nv);
        EXPECT_EQ(low, val.table[1]);
        EXPECT_EQ(high, val.table[0]);
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(10, sin.readInt());
        EXPECT_EQ('A', sin.readChar());
        EXPECT_EQ('B', sin.readChar());
        EXPECT_EQ('C', sin.readChar());
        EXPECT_EQ('D', sin.readChar());
        EXPECT_EQ('E', sin.readChar());
        EXPECT_EQ('a', sin.readChar());
        EXPECT_EQ('b', sin.readChar());
        EXPECT_EQ('c', sin.readChar());
        EXPECT_EQ('d', sin.readChar());
        EXPECT_EQ('e', sin.readChar());
    }
    if (cnt-- >= 0)
    {
        EXPECT_EQ(20, sin.readInt());
        for (int ii =0; ii < 4; ++ii) {
            EXPECT_EQ('a', sin.readChar());
            EXPECT_EQ('b', sin.readChar());
            EXPECT_EQ('c', sin.readChar());
            EXPECT_EQ('d', sin.readChar());
            EXPECT_EQ('e', sin.readChar());
        }
    }
}

/*
 * Verify that tuple serialization produces expected content
 */
TEST_F(TableTupleExportTest, serToExport)
{
    uint8_t nulls[1] = {0};
    char data[2048];
    memset(data, 0, 2048);

    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // create a schema by selecting a column from the super-set.

    // tinyiny
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(1, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(3, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + integer
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(7, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + bigint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(15, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + timestamp
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(23, sz);
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + decimal
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(41, sz);  // length, radix pt, sign, prec.
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + first varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(55, sz); // length, 10 chars
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);

    // + second varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data);
    EXPECT_EQ(79, sz); // length, 20 chars
    EXPECT_EQ(0x0, nulls[0]);  // all null
    verSer(i-1, data);
}


/* verify serialization of nulls */
TEST_F(TableTupleExportTest, serWithNulls)
{

    uint8_t nulls[1] = {0};
    char data[2048];
    memset(data, 0, 2048);

    size_t sz = 0;
    std::vector<uint16_t> keep_offsets;
    uint16_t i = 0;

    // tinyiny
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80, nulls[0]);

    // tinyint + smallint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40, nulls[0]);  // all null

    // + integer
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20, nulls[0]);  // all null

    // + bigint
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10, nulls[0]);  // all null

    // + timestamp
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8, nulls[0]);  // all null

    // + decimal
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz);  // length, radix pt, sign, prec.
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4, nulls[0]);  // all null

    // + first varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz); // length, 10 chars
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4 | 0x2, nulls[0]);  // all null

    // + second varchar
    keep_offsets.push_back(i++);
    sz = serElSize(keep_offsets, nulls, data, true);
    EXPECT_EQ(0, sz); // length, 20 chars
    EXPECT_EQ(0x80 | 0x40 | 0x20 | 0x10 | 0x8 | 0x4 | 0x2 | 0x1, nulls[0]);  // all null
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
