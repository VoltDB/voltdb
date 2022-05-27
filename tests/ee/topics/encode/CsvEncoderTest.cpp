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

#include "topics/encode/CsvEncoder.h"
#include "topics/encode/EncoderTestBase.h"

using namespace voltdb;
using namespace voltdb::topics;

class CsvEncoderTest : public EncoderTestBase {
protected:
    void testString(const std::string& input, const std::string& expected);
};

// Insert only the input string into the tuple and compare its csv encoding with the expected value
void CsvEncoderTest::testString(const std::string& input, const std::string& expected) {
    setupAllSchema(true);
    m_tuple.setAllNulls();
    m_tuple.setNValue(7, ValueFactory::getStringValue(input, &m_pool));

    std::vector<int32_t> indexes { 7 };
    CsvEncoder csve(indexes, std::unordered_map<std::string, std::string>());

    int32_t size = csve.sizeOf(m_tuple);
    std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
    ReferenceSerializeOutput out(encoded.get(), size);

    csve.encode(out, m_tuple);
    std::string encoded_out(out.data(), out.size());
    ASSERT_EQ(expected, encoded_out);
}

TEST_F(CsvEncoderTest, QuoteComma) {
    testString("i really, should be quoted", "\"i really, should be quoted\"");
}

TEST_F(CsvEncoderTest, QuoteNewline) {
    testString("i really\nshould be quoted but NOT escaped", "\"i really\nshould be quoted but NOT escaped\"");
}

TEST_F(CsvEncoderTest, QuoteCarriageReturn) {
    testString("i really\rshould be quoted but NOT escaped", "\"i really\rshould be quoted but NOT escaped\"");
}

TEST_F(CsvEncoderTest, QuoteQuote) {
    testString("i really\"should be quoted and escaped", "\"i really\\\"should be quoted and escaped\"");
}

// Basic test that serialization of fields which cannot be null works
TEST_F(CsvEncoderTest, BasicNonNullableCsv) {
    setupAllSchema(false);

    GeographyPointValue point(12.5, 78.9);

    std::vector<std::unique_ptr<S2Loop> > loops;
    std::vector<S2Point> points( {S2Point(50, 5000, 100), S2Point(40, 900, 50), S2Point(900, 2000, 300)});
    loops.emplace_back(new S2Loop(points));
    Polygon geography;
    geography.init(&loops, false);

    std::string varchar = "   some silly string";
    std::vector<uint8_t> binary { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    // Insert value with the 6th (timestamp) raised to a millisecond value
    insertValues(1, 2, 3, 4, 5, (6 * 1000), 7, varchar, binary, &point, &geography);

    // Verify default encoding
    std::vector<int32_t> indexes { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    CsvEncoder csve(indexes, std::unordered_map<std::string, std::string>());

    int32_t size = csve.sizeOf(m_tuple);
    std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
    ReferenceSerializeOutput out(encoded.get(), size);

    csve.encode(out, m_tuple);
    std::string encoded_out(out.data(), out.size());

    // Compare against expected
    std::string expected = {"1,2,3,4,5.00000000000000000,1970-01-01 00:00:00.006,7.000000000000,   some silly string,0102030405060708090A,POINT (12.5 78.9),\"POLYGON ((89.427061302317 1.145705569599, 87.455195620187 3.176700617784, 65.772254682046 7.789047734178, 89.427061302317 1.145705569599))\""};
    ASSERT_EQ(expected, encoded_out);

    // Verify all quoted encoding
    std::unordered_map<std::string, std::string> props;
    props[CsvEncoder::PROP_CSV_QUOTE_ALL] = std::string("true");
    CsvEncoder csve_quoted( indexes, props);

    int32_t size_quoted = csve_quoted.sizeOf(m_tuple);
    std::unique_ptr<uint8_t[]> encoded_quoted(new uint8_t[size_quoted]);
    ReferenceSerializeOutput out_quoted(encoded_quoted.get(), size_quoted);

    csve_quoted.encode(out_quoted, m_tuple);
    std::string encoded_out_quoted(out_quoted.data(), out_quoted.size());

    std::string expected_quoted {"\"1\",\"2\",\"3\",\"4\",\"5.00000000000000000\",\"1970-01-01 00:00:00.006\",\"7.000000000000\",\"   some silly string\",\"0102030405060708090A\",\"POINT (12.5 78.9)\",\"POLYGON ((89.427061302317 1.145705569599, 87.455195620187 3.176700617784, 65.772254682046 7.789047734178, 89.427061302317 1.145705569599))\""};
    ASSERT_EQ(expected_quoted, encoded_out_quoted);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
