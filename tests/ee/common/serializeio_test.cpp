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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <limits>
#include <string>
#include "harness.h"
#include "common/serializeio.h"

using namespace std;
using namespace voltdb;

class SerializeIOTest : public Test {
public:
    SerializeIOTest() : TEXT("hello world") {}
protected:
    const string TEXT;
    void writeTestSuite(SerializeOutput* out) {
        out->writeBool(true);
        out->writeBool(false);
        out->writeByte(numeric_limits<int8_t>::min());
        out->writeByte(numeric_limits<int8_t>::max());
        out->writeShort(numeric_limits<int16_t>::min());
        out->writeShort(numeric_limits<int16_t>::max());
        out->writeInt(numeric_limits<int32_t>::min());
        out->writeInt(numeric_limits<int32_t>::max());
        out->writeLong(numeric_limits<int64_t>::min());
        out->writeLong(numeric_limits<int64_t>::max());
        out->writeFloat(numeric_limits<float>::min());
        out->writeFloat(numeric_limits<float>::max());
        out->writeDouble(numeric_limits<double>::min());
        out->writeDouble(numeric_limits<double>::max());
        out->writeTextString(TEXT);
    }

    void readTestSuite(SerializeInputBE* in) {
        EXPECT_EQ(true, in->readBool());
        EXPECT_EQ(false, in->readBool());
        EXPECT_EQ(numeric_limits<int8_t>::min(), in->readByte());
        EXPECT_EQ(numeric_limits<int8_t>::max(), in->readByte());
        EXPECT_EQ(numeric_limits<int16_t>::min(), in->readShort());
        EXPECT_EQ(numeric_limits<int16_t>::max(), in->readShort());
        EXPECT_EQ(numeric_limits<int32_t>::min(), in->readInt());
        EXPECT_EQ(numeric_limits<int32_t>::max(), in->readInt());
        EXPECT_EQ(numeric_limits<int64_t>::min(), in->readLong());
        EXPECT_EQ(numeric_limits<int64_t>::max(), in->readLong());
        EXPECT_EQ(numeric_limits<float>::min(), in->readFloat());
        EXPECT_EQ(numeric_limits<float>::max(), in->readFloat());
        EXPECT_EQ(numeric_limits<double>::min(), in->readDouble());
        EXPECT_EQ(numeric_limits<double>::max(), in->readDouble());
        EXPECT_EQ(TEXT, in->readTextString());
    }
};

TEST_F(SerializeIOTest, ReadWrite) {
    CopySerializeOutput out;
    writeTestSuite(&out);

    ReferenceSerializeInputBE in(out.data(), out.size());
    readTestSuite(&in);

    CopySerializeInputBE in2(out.data(), out.size());
    memset(const_cast<char*>(out.data()), 0, out.size());
    readTestSuite(&in2);
}

TEST_F(SerializeIOTest, Unread) {
    static const char data[] = { 1, 2, 3, 4};
    ReferenceSerializeInputBE in(data, sizeof(data));
    EXPECT_EQ(0x01020304, in.readInt());
    // Read the int again
    in.unread(sizeof(int32_t));
    EXPECT_EQ(0x01020304, in.readInt());
}

TEST(SerializeOutput, ReserveBytes) {
    CopySerializeOutput out;
    size_t offset = out.reserveBytes(4);
    EXPECT_EQ(0, offset);
    EXPECT_EQ(4, out.size());

    static const uint32_t DATA = 0x01020304;
    // Writing past the end = bad
    EXPECT_DEATH(out.writeBytesAt(1, &DATA, sizeof(DATA)));

    size_t offset2 = out.reserveBytes(5);
    EXPECT_EQ(4, offset2);
    EXPECT_EQ(9, out.size());

    size_t nextOffset = out.writeBytesAt(1, &DATA, sizeof(DATA));
    EXPECT_EQ(1+sizeof(DATA), nextOffset);
    EXPECT_EQ(0, memcmp(static_cast<const char*>(out.data()) + 1, &DATA, sizeof(DATA)));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
