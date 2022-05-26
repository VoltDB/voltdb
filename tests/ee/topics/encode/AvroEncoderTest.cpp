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
#include "topics/encode/AvroEncoder.h"
#include "topics/encode/AvroTestUtils.h"

#include "EncoderTestBase.h"

using namespace voltdb;
using namespace voltdb::topics;

class AvroEncoderTest : public EncoderTestBase {
};

// Test that serialization of fields which cannot be null works
TEST_F(AvroEncoderTest, NonNullableAvro) {
    setupAllSchema(false);

    std::vector<int32_t> indexes { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    AvroEncoder ae(25, *m_schema, indexes, std::unordered_map<std::string, std::string>());
    GeographyPointValue point(12.5, 78.9);

    // The dumbest geography encoding I could figure out
    std::string varchar = "some silly string";
    std::vector<uint8_t> binary { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    std::vector<std::unique_ptr<S2Loop> > loops;
    std::vector<S2Point> points( {S2Point(50, 5000, 100), S2Point(40, 900, 50), S2Point(900, 2000, 300)});
    loops.emplace_back(new S2Loop(points));
    Polygon geography;
    geography.init(&loops, false);
    insertValues(1, 2, 3, 6092398495938644137L, 5, 6, 7, varchar, binary, &point, &geography);

    int32_t size = ae.sizeOf(m_tuple);
    std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);

    ReferenceSerializeOutput out(encoded.get(), static_cast<size_t>(size));
    int32_t written = ae.encode(out, m_tuple);
    ASSERT_EQ(ae.sizeOf(m_tuple), written);
    ASSERT_EQ(written, out.position());

    ReferenceSerializeInputBE in(encoded.get(), written);
    validateHeader(in, 25);
    ASSERT_EQ(1, in.readVarInt()); // tinyint
    ASSERT_EQ(2, in.readVarInt()); // smallint
    ASSERT_EQ(3, in.readVarInt()); // integer
    ASSERT_EQ(6092398495938644137L, in.readVarLong()); // bigint
    ASSERT_EQ(5, readAvroDouble(in)); // double
    ASSERT_EQ(6, in.readVarLong()); // timestamp

    ASSERT_EQ(16, in.readVarInt()); // Size of decimal
    int64_t val = in.readLong();
    ASSERT_EQ(0, ntohll(val)); // Highest significant bits
    val = in.readLong();
    ASSERT_EQ(7000000000000, ntohll(val)); // Lowest significant bits with scale of 12

    // Validate the string  can be deserialized
    int64_t len = in.readVarInt();
    ASSERT_EQ(varchar.length(), len);
    std::unique_ptr<char[]> strDecoded(new char[len]);
    in.readBytes(strDecoded.get(), len);
    ASSERT_EQ(varchar, std::string(strDecoded.get(), len));

    // Validate the var binary can be deserialized
    len = in.readVarInt();
    ASSERT_EQ(binary.size(), len);
    std::unique_ptr<char[]> binaryDecoded(new char[len]);
    in.readBytes(binaryDecoded.get(), len);
    ASSERT_EQ(0, ::memcmp(binary.data(), binaryDecoded.get(), len));

    // Validate the point
    ASSERT_EQ(point.getLongitude(), in.readDouble());
    ASSERT_EQ(point.getLatitude(), in.readDouble());

    // Validate the geography
    len = in.readVarInt();
    ASSERT_EQ(geography.serializedLength(), len);
    std::unique_ptr<char[]> geoBytes(new char[len]);
    in.readBytes(geoBytes.get(), len);
    GeographyValue geographyDecoded(geoBytes.get(), len);
    ASSERT_EQ(0, ValuePeeker::peekGeographyValue(m_tuple.getNValue(10)).compareWith(geographyDecoded));

    ASSERT_EQ(0, in.remaining());
}

// Test that serialization of fields which can be null works
TEST_F(AvroEncoderTest, NullableAvro) {
    setupAllSchema(true);

    std::vector<int32_t> indexes { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    AvroEncoder ae(25, *m_schema, indexes, std::unordered_map<std::string, std::string>());
    GeographyPointValue point(12.5, 78.9);

    // The dumbest geography encoding I could figure out
    std::string varchar = "some silly string";
    std::vector<uint8_t> binary { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    std::vector<std::unique_ptr<S2Loop> > loops;
    std::vector<S2Point> points( {S2Point(50, 5000, 100), S2Point(40, 900, 50), S2Point(900, 2000, 300)});
    loops.emplace_back(new S2Loop(points));
    Polygon geography;
    geography.init(&loops, false);
    insertValues(1, 2, 3, 6092398495938644137L, 5, 6, 7, varchar, binary, &point, &geography);

    int32_t size = ae.sizeOf(m_tuple);
    std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);

    ReferenceSerializeOutput out(encoded.get(), static_cast<size_t>(size));
    int32_t written = ae.encode(out, m_tuple);
    ASSERT_EQ(ae.sizeOf(m_tuple), written);
    ASSERT_EQ(written, out.position());

    ReferenceSerializeInputBE in(encoded.get(), written);
    validateHeader(in, 25);

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(1, in.readVarInt()); // tinyint

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(2, in.readVarInt()); // smallint

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(3, in.readVarInt()); // integer

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(6092398495938644137L, in.readVarLong()); // bigint

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(5, readAvroDouble(in)); // double

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(6, in.readVarLong()); // timestamp

    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(16, in.readVarLong()); // Size of decimal
    int64_t val = in.readLong();
    ASSERT_EQ(0, ntohll(val)); // Highest significant bits
    val = in.readLong();
    ASSERT_EQ(7000000000000, ntohll(val)); // Lowest significant bits with scale of 12

    // Validate the string  can be deserialized
    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    int64_t len = in.readVarInt();
    ASSERT_EQ(varchar.length(), len);
    std::unique_ptr<char[]> strDecoded(new char[len]);
    in.readBytes(strDecoded.get(), len);
    ASSERT_EQ(varchar, std::string(strDecoded.get(), len));

    // Validate the var binary can be deserialized
    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    len = in.readVarInt();
    ASSERT_EQ(binary.size(), len);
    std::unique_ptr<char[]> binaryDecoded(new char[len]);
    in.readBytes(binaryDecoded.get(), len);
    ASSERT_EQ(0, ::memcmp(binary.data(), binaryDecoded.get(), len));

    // Validate the point
    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    ASSERT_EQ(point.getLongitude(), in.readDouble());
    ASSERT_EQ(point.getLatitude(), in.readDouble());

    // Validate the geography
    ASSERT_EQ(1, in.readVarInt()); // Union index indicating not null
    len = in.readVarInt();
    ASSERT_EQ(geography.serializedLength(), len);
    std::unique_ptr<char[]> geoBytes(new char[len]);
    in.readBytes(geoBytes.get(), len);
    GeographyValue geographyDecoded(geoBytes.get(), len);
    ASSERT_EQ(0, ValuePeeker::peekGeographyValue(m_tuple.getNValue(10)).compareWith(geographyDecoded));

    ASSERT_EQ(0, in.remaining());
}

// Test that nulls of all types are correctly serialized
TEST_F(AvroEncoderTest, AllNullAvro) {
    setupAllSchema(true);
    m_tuple.setAllNulls();

    std::vector<int32_t> indexes { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    AvroEncoder ae(25, *m_schema, indexes, std::unordered_map<std::string, std::string>());
    int32_t size = ae.sizeOf(m_tuple);
    std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);

    ReferenceSerializeOutput out(encoded.get(), static_cast<size_t>(size));
    int32_t written = ae.encode(out, m_tuple);
    ASSERT_EQ(written, out.position());

    ReferenceSerializeInputBE in(encoded.get(), size);
    validateHeader(in, 25);

    // Validate all union indexers are 0 which indicates null
    for (int i = 0; i < m_schema->columnCount(); ++i) {
        ASSERT_EQ(0, in.readVarInt());
    }

    ASSERT_EQ(0, in.remaining());
}

// Test the encoding a subset of columns in any order works
TEST_F(AvroEncoderTest, SomeColumnsEncoded) {
    setupAllSchema(false);

    GeographyPointValue point(12.5, 78.9);

    // The dumbest geography encoding I could figure out
    std::string varchar = "some silly string";
    std::vector<uint8_t> binary { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    std::vector<std::unique_ptr<S2Loop> > loops;
    std::vector<S2Point> points( {S2Point(50, 5000, 100), S2Point(40, 900, 50), S2Point(900, 2000, 300)});
    loops.emplace_back(new S2Loop(points));
    Polygon geography;
    geography.init(&loops, false);
    insertValues(1, 2, 3, 4, 5, 6, 7, varchar, binary, &point, &geography);

    {
        std::vector<int32_t> indexes { 1, 3, 5,};
        AvroEncoder ae(90, *m_schema, indexes, std::unordered_map<std::string, std::string>());

        int32_t size = ae.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);

        ReferenceSerializeOutput out(encoded.get(), static_cast<size_t>(size));
        int32_t written = ae.encode(out, m_tuple);
        ASSERT_EQ(written, out.position());

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 90);

        ASSERT_EQ(2, in.readVarInt()); // smallint
        ASSERT_EQ(4, in.readVarLong()); // bigint
        ASSERT_EQ(6, in.readVarLong()); // timestamp
    }

    {
        std::vector<int32_t> indexes { 5, 3, 1,};
        AvroEncoder ae(90, *m_schema, indexes, std::unordered_map<std::string, std::string>());

        int32_t size = ae.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);

        ReferenceSerializeOutput out(encoded.get(), static_cast<size_t>(size));
        int32_t written = ae.encode(out, m_tuple);
        ASSERT_EQ(written, out.position());

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 90);

        ASSERT_EQ(6, in.readVarLong()); // timestamp
        ASSERT_EQ(4, in.readVarLong()); // bigint
        ASSERT_EQ(2, in.readVarInt()); // smallint
    }
}

// Test the different encodings of timestamp work
TEST_F(AvroEncoderTest, TimestampEncoding) {
    std::vector<ValueType> types { ValueType::tTIMESTAMP };
    std::vector<int32_t> sizes { 8 };
    std::vector<bool> nullable { false };
    m_schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullable);
    setupTuple();

    const int64_t time = 1607698898846;
    m_tuple.setNValue(0, ValueFactory::getTimestampValue(time));

    std::unordered_map<std::string, std::string> props;
    props[AvroEncoder::PROP_TIMESTAMP_ENCODING] = std::string("MICROSECONDS");

    std::vector<int32_t> indexes { 0 };
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(time, in.readVarLong());
    }

    props[AvroEncoder::PROP_TIMESTAMP_ENCODING] = std::string("MILLISECONDS");
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(time / 1000, in.readVarLong());
    }
}

// Test the different encodings of a point work
TEST_F(AvroEncoderTest, GeographyPointEncoding) {
    std::vector<ValueType> types { ValueType::tPOINT };
    std::vector<int32_t> sizes { 16 };
    std::vector<bool> nullable { false };
    m_schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullable);
    setupTuple();

    GeographyPointValue point(78.5, -25.98);
    m_tuple.setNValue(0, ValueFactory::getGeographyPointValue(&point));

    std::unordered_map<std::string, std::string> props;
    props[AvroEncoder::PROP_POINT_ENCODING] = std::string("FIXED_BINARY");

    std::vector<int32_t> indexes { 0 };
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(point.getLongitude(), in.readDouble());
        ASSERT_EQ(point.getLatitude(), in.readDouble());
    }

    props[AvroEncoder::PROP_POINT_ENCODING] = std::string("BINARY");
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(16, in.readVarInt()); // size of encoding
        ASSERT_EQ(point.getLongitude(), in.readDouble());
        ASSERT_EQ(point.getLatitude(), in.readDouble());
    }

    props[AvroEncoder::PROP_POINT_ENCODING] = std::string("STRING");
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        std::string string = point.toWKT();

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(string.length(), in.readVarInt()); // size of encoding
        std::unique_ptr<char[]> strBytes(new char[string.length()]);
        in.readBytes(strBytes.get(), string.length());
        ASSERT_EQ(string, std::string(strBytes.get(), string.length()));
    }
}

// Test the different encodings of geography value work
TEST_F(AvroEncoderTest, GeographyEncoding) {
    std::vector<ValueType> types { ValueType::tGEOGRAPHY };
    std::vector<int32_t> sizes { 512 };
    std::vector<bool> nullable { false };
    m_schema = TupleSchema::createTupleSchemaForTest(types, sizes, nullable);
    setupTuple();

    std::vector<std::unique_ptr<S2Loop> > loops;
    std::vector<S2Point> points( {S2Point(50, 5000, 100), S2Point(40, 900, 50), S2Point(900, 2000, 300)});
    loops.emplace_back(new S2Loop(points));
    Polygon polygon;
    polygon.init(&loops, false);
    NValue nvalue = ValueFactory::getGeographyValue(&polygon, &m_pool);
    GeographyValue geography = ValuePeeker::peekGeographyValue(nvalue);
    m_tuple.setNValue(0, nvalue);

    std::unordered_map<std::string, std::string> props;
    props[AvroEncoder::PROP_GEOGRAPHY_ENCODING] = std::string("BINARY");

    std::vector<int32_t> indexes { 0 };
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(geography.length(), in.readVarInt());
        std::unique_ptr<char[]> geoBytes(new char[geography.length()]);
        in.readBytes(geoBytes.get(), geography.length());
        ASSERT_EQ(0, ::memcmp(geography.data(), geoBytes.get(), geography.length()));
    }

    props[AvroEncoder::PROP_GEOGRAPHY_ENCODING] = std::string("STRING");
    {
        AvroEncoder encoder(30, *m_schema, indexes, props);
        int32_t size = encoder.sizeOf(m_tuple);
        std::unique_ptr<uint8_t[]> encoded(new uint8_t[size]);
        ReferenceSerializeOutput out(encoded.get(), size);

        encoder.encode(out, m_tuple);

        std::string string = ValuePeeker::peekGeographyValue(m_tuple.getNValue(0)).toWKT();

        ReferenceSerializeInputBE in(encoded.get(), size);
        validateHeader(in, 30);
        ASSERT_EQ(string.length(), in.readVarInt()); // size of encoding
        std::unique_ptr<char[]> strBytes(new char[string.length()]);
        in.readBytes(strBytes.get(), string.length());
        ASSERT_EQ(string, std::string(strBytes.get(), string.length()));
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
