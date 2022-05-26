/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <memory>
#include <sstream>
#include <vector>

#include "boost/functional/hash.hpp"

#include "common/serializeio.h"
#include "common/value_defs.h"
#include "GeographyPointValue.hpp"
#include "s2geo/s2loop.h"
#include "s2geo/s2polygon.h"

namespace voltdb {

class GeographyValue;

/**
 * A subclass of S2Loop that allows instances to be initialized from
 * the EE's serializer classes.
 */
class Loop : public S2Loop {
public:

    Loop() = default;
    template<class Deserializer>
    void initFromBuffer(Deserializer& input, bool doRepairs = false);

    template<class Serializer>
    void saveToBuffer(Serializer& output) const;

    template<class Serializer, class Deserializer>
    static void copyViaSerializers(Serializer& output, Deserializer& input);

    template<class Deserializer>
    static void pointArrayFromBuffer(Deserializer& input, std::vector<S2Point>* points);

    static std::size_t serializedLength(int32_t numVertices);
};

/**
 * A subclass of S2Polygon that allows instances to be initialized from
 * the EE's serializer classes.
 */
class Polygon : public S2Polygon {
public:

    Polygon() = default;
    void init(std::vector<std::unique_ptr<S2Loop> > *loops, bool doRepairs);

    void initFromGeography(const GeographyValue& geog, bool doRepairs = false);

    template<class Deserializer>
    void initFromBuffer(Deserializer& input, bool doRepairs = false);

    template<class Serializer>
    void saveToBuffer(Serializer& output) const;

    template<class Serializer, class Deserializer>
    static void copyViaSerializers(Serializer& output, Deserializer& input);

    static std::size_t serializedLengthNoLoops();

    std::size_t serializedLength() const;

    double getDistance(const GeographyPointValue &point) {
        const S2Point s2Point = point.toS2Point();
        S1Angle distanceRadians = S1Angle(Project(s2Point), s2Point);
        return distanceRadians.radians();
    }
};

/**
 * A class for representing instances of geo-spatial geographies.
 * (Currently only polygons can be represented here.)
 *
 * Note that variable length data in the EE is typically prefixed with
 * a 4-byte integer that is the length of the data in bytes.  The
 * pointer accepted by the constructor here should be to the start of
 * the data just after the length.
 */
class GeographyValue {
    const char* m_data = nullptr;
    int32_t m_length = 0;
public:

    /** Constructor for a null geography */
    GeographyValue() = default;

    GeographyValue(const char* data, int32_t length)
        : m_data(data) , m_length(length) { }

    bool isNull() const {
        return m_data == nullptr;
    }

    const char* data() const {
        return m_data;
    }

    int32_t length() const {
        return m_length;
    }

    /**
     * Do a comparison with another geography (polygon).
     */
    int compareWith(const GeographyValue& rhs) const;

    /**
     * Serialize this geography
     */
    template<class Serializer>
    void serializeTo(Serializer& output) const;

    /**
     * Populate a pointer to storage with the bytes that represent a
     * geography.  Note that the caller has already read the
     * length-prefix that accompanies variable-length data and sized
     * the target storage appropriately.
     */
    template<class Deserializer>
    static void deserializeFrom(Deserializer& input, char* storage, int32_t length);

    /**
     * Hash this geography value (used for hash aggregation where a
     * geography is group by key).
     */
    void hashCombine(std::size_t& seed) const;

    /**
     * Produce a human-readable summary of this geography
     */
    std::string toString() const;
    // returns wkt representation for given polygon:
    // "POLYGON ((<Longitude> <Latitude>, <Longitude> <Latitude> .. <Longitude> <Latitude>)[,(..), (..),..(..)])"
    std::string toWKT() const;
};

/**
 * A class similar to ReferenceSerializeOutput, except that it doesn't
 * do any byte-swapping.
 */
class SimpleOutputSerializer {
    template<typename T>
    void writeNative(T val) {
        vassert(m_cursor - m_buffer < m_size);
        reinterpret_cast<T*>(m_cursor)[0] = val;
        m_cursor += sizeof(T);
    }

    char* const m_buffer;
    const std::size_t m_size;

    char* m_cursor;
public:
    SimpleOutputSerializer(char* const buffer, std::size_t size)
        : m_buffer(buffer) , m_size(size) , m_cursor(buffer) { }

    ~SimpleOutputSerializer() {
        // make sure we consumed everything we expected to.
        vassert(m_buffer + m_size == m_cursor);
    }

    void writeByte(int8_t byte) {
        writeNative(byte);
    }

    void writeBool(bool val) {
        writeNative(static_cast<int8_t>(val));
    }

    void writeInt(int32_t val) {
        writeNative(val);
    }

    void writeDouble(double val) {
        writeNative(val);
    }

    void writeBinaryString(const void* value, size_t length) {
        ::memcpy(m_cursor, value, length);
        m_cursor += length;
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "SimpleOutputSerializer with buffer size " << m_size
            << ", current offset = " << (m_cursor - m_buffer);
        return oss.str();
    }
};

inline int GeographyValue::compareWith(const GeographyValue& rhs) const {
    /* Do floating-point comparisons only as a last resort to help
     * avoid issues with floating-point math.  It doesn't really
     * matter how we do our comparison as long as we produce a
     * deterministic order.
     *
     *   1. First compare number of loops (polygons with fewer loops sort as smaller).
     *   2. If the number of loops are the same, compare the number
     *        of vertices in each loop.  The polygon with first loop
     *        containing fewer vertices will sort as smaller.
     *   3. Finally, if all loops have the same number of vertices, sort on the
     *        points themselves (which will involve doing floating-point comparison)
     */

    Polygon lhsPoly;
    lhsPoly.initFromGeography(*this);

    Polygon rhsPoly;
    rhsPoly.initFromGeography(rhs);

    if (lhsPoly.num_loops() < rhsPoly.num_loops()) {
        return VALUE_COMPARE_LESSTHAN;
    } else if (lhsPoly.num_loops() > rhsPoly.num_loops()) {
        return VALUE_COMPARE_GREATERTHAN;
    }

    // number of loops are the same.
    // compare on number of vertices in each loop
    for (int i = 0; i < lhsPoly.num_loops(); ++i) {
        S2Loop* lhsLoop = lhsPoly.loop(i);
        S2Loop* rhsLoop = rhsPoly.loop(i);

        if (lhsLoop->num_vertices() < rhsLoop->num_vertices()) {
            return VALUE_COMPARE_LESSTHAN;
        } else if (lhsLoop->num_vertices() > rhsLoop->num_vertices()) {
            return VALUE_COMPARE_GREATERTHAN;
        }
    }

    // Each loop has the same number of vertices.
    // Compare the vertices themselves.
    for (int i = 0; i < lhsPoly.num_loops(); ++i) {
        S2Loop* lhsLoop = lhsPoly.loop(i);
        S2Loop* rhsLoop = rhsPoly.loop(i);

        for (int j = 0; j < lhsLoop->num_vertices(); ++j) {
            const GeographyPointValue lhsVert(lhsLoop->vertex(j));
            const GeographyPointValue rhsVert(rhsLoop->vertex(j));
            int cmp = lhsVert.compareWith(rhsVert);
            if (cmp != VALUE_COMPARE_EQUAL) {
                return cmp;
            }
        }
    }

    return VALUE_COMPARE_EQUAL;
}


inline void GeographyValue::hashCombine(std::size_t& seed) const {

    if (isNull()) {
        // Treat a null as a polygon with zero loops
        boost::hash_combine(seed, 0);
        return;
    }

    Polygon poly;
    poly.initFromGeography(*this);

    int numLoops = poly.num_loops();
    boost::hash_combine(seed, numLoops);
    for (int i = 0; i < numLoops; ++i) {
        S2Loop* loop = poly.loop(i);
        for (int j = 0; j < loop->num_vertices(); ++j) {
            const S2Point& v = loop->vertex(j);
            boost::hash_combine(seed, v.x());
            boost::hash_combine(seed, v.y());
            boost::hash_combine(seed, v.z());
        }
    }
}

inline std::string GeographyValue::toString() const {
    std::ostringstream oss;

    if (isNull()) {
        oss << "null polygon";
    } else {
        Polygon poly;
        poly.initFromGeography(*this);
        int numLoops = poly.num_loops();
        oss << "polygon with "
            << numLoops << " loops with vertex counts";
        for (int i = 0; i < numLoops; ++i) {
            oss << " " << poly.loop(i)->num_vertices()
                << " (depth=" << poly.loop(i)->depth() << ")";
        }
    }

    return oss.str();
}

inline std::string GeographyValue::toWKT() const {
    vassert(!isNull());

    Polygon poly;
    poly.initFromGeography(*this);
    int numLoops = poly.num_loops();
    vassert(numLoops > 0);
    GeographyPointValue point;

    std::ostringstream oss;
    oss << "POLYGON (";
    // Note that we need to reverse the order of holes,
    // but not of shells.
    bool is_shell = true;
    // capture all the loops
    for (int i = 0; i < numLoops; ++i) {
        const S2Loop *loop = poly.loop(i);
        const int numVertices = loop->num_vertices();
        vassert(numVertices >= 3); // each loop will be composed of at least 3 vertices. This does not include repeated end vertex
        oss << "(";
        // Capture the first point first.  This is always
        // First, even if this is a hole or a shell.
        point = GeographyPointValue(loop->vertex(0));
        oss << point.formatLngLat() << ", ";
        int startIdx = (is_shell ? 1 : numVertices-1);
        int endIdx   = (is_shell ? numVertices : 0);
        int delta    = (is_shell ? 1 : -1);
        for (int j = startIdx; j != endIdx; j += delta) {
            point = GeographyPointValue(loop->vertex(j));
            oss << point.formatLngLat() << ", ";
        }
        // repeat the first vertex to close the loop
        point = GeographyPointValue(loop->vertex(0));
        oss << point.formatLngLat() << ")";
        // last loop?
        if (i < numLoops -1) {
            oss << ", " ;
        }
        is_shell = false;
    }
    oss << ")";

    return oss.str();
}

template<class Deserializer>
inline void GeographyValue::deserializeFrom(Deserializer& input, char* storage, int32_t length) {
    SimpleOutputSerializer output(storage, length);
    Polygon::copyViaSerializers(output, input);
}

template<class Serializer>
inline void GeographyValue::serializeTo(Serializer& output) const {
    ReferenceSerializeInputLE input(m_data, m_length);
    Polygon::copyViaSerializers(output, input);
}


const int8_t INCOMPLETE_ENCODING_FROM_JAVA = 0;
const int8_t COMPLETE_ENCODING = 1;

const std::size_t BOUND_SERIALIZED_SIZE =
    sizeof(int8_t)  // encoding version
    + (sizeof(double) * 4); // 2 corners of bounding box, as min/max lat/lng


template<class Deserializer>
static inline void initBoundFromBuffer(S2LatLngRect *bound, Deserializer& input) {
    input.readByte(); // encoding version

    double latLo = input.readDouble();
    double latHi = input.readDouble();
    double lngLo = input.readDouble();
    double lngHi = input.readDouble();
    *bound = S2LatLngRect(R1Interval(latLo, latHi), S1Interval(lngLo, lngHi));
}

template<class Serializer>
static inline void saveBoundToBuffer(const S2LatLngRect *bound, Serializer& output) {
    output.writeByte(COMPLETE_ENCODING);
    output.writeDouble(bound->lat().lo());
    output.writeDouble(bound->lat().hi());
    output.writeDouble(bound->lng().lo());
    output.writeDouble(bound->lng().hi());
}

template<class Serializer, class Deserializer>
static inline void copyBoundViaSerializers(Serializer& output, Deserializer& input) {
    output.writeByte(input.readByte()); // encoding version
    for (int i = 0; i < 4; ++i) {
        output.writeDouble(input.readDouble());
    }
}

template<class Deserializer>
static inline void skipBound(Deserializer& input) {
    input.readByte();
    input.readDouble();
    input.readDouble();
    input.readDouble();
    input.readDouble();
}




inline std::size_t Loop::serializedLength(int32_t numVertices) {
    return
        sizeof(int8_t) + // encoding version
        sizeof(int32_t) + // num vertices
        (numVertices * 3 * sizeof(double)) + // vertices
        sizeof(int8_t) + // origin inside
        sizeof(int32_t) + // depth
        BOUND_SERIALIZED_SIZE;
}

template<class Serializer, class Deserializer>
inline void Loop::copyViaSerializers(Serializer& output, Deserializer& input) {
    output.writeByte(input.readByte()); // encoding version
    int numVertices = input.readInt();
    output.writeInt(numVertices);
    for (int i = 0; i < numVertices; ++i) {
        output.writeDouble(input.readDouble());
        output.writeDouble(input.readDouble());
        output.writeDouble(input.readDouble());
    }

    output.writeByte(input.readByte()); // origin inside
    int32_t depth = input.readInt();
    output.writeInt(depth);

    copyBoundViaSerializers(output, input);
}

template<class Deserializer>
inline void Loop::pointArrayFromBuffer(Deserializer& input, std::vector<S2Point>* points) {
    input.readByte(); // encoding version
    int numVertices = input.readInt();
    for (int i = 0; i < numVertices; ++i) {
        double x = input.readDouble();
        double y = input.readDouble();
        double z = input.readDouble();
        points->push_back(S2Point(x, y, z));
    }
    input.readByte(); // origin inside
    input.readInt();  // depth

    skipBound(input);
}

template<class Deserializer>
inline void Loop::initFromBuffer(Deserializer& input, bool doRepairs) {
    input.readByte();

    set_num_vertices(input.readInt());

    S2Point *src = reinterpret_cast<S2Point*>(
                            const_cast<char*>(
                                    input.getRawPointer(num_vertices() * sizeof(S2Point))));
    int8_t origin_inside = input.readByte();
    int32_t thedepth = input.readInt();
    S2LatLngRect bound;
    initBoundFromBuffer(&bound, input);
    // Do this before doing (potentially) the inversions.
    /*
     * If we are going to do repairs, potentially,
     * we want to take command of our own vertices.
     */
    if (doRepairs) {
        set_owns_vertices(true);
        set_origin_inside(origin_inside);
        set_depth(thedepth);
        set_rect_bound(bound);
        Init(src, num_vertices());
        // If this loop is already normalized, this
        // will not do anything.  If it is not it will
        // invert the loop.
        Normalize(true);
    } else {
        // Point these vertices at the vertices
        // in the tuple.  This loop does not
        // own these vertices, so we won't delete
        // them when the loop is reaped.
        vassert(!owns_vertices());
        set_vertices(src);
        set_origin_inside(origin_inside);
        set_depth(thedepth);
        set_rect_bound(bound);
    }
    vassert(depth() >= 0);
}

template<class Serializer>
void Loop::saveToBuffer(Serializer& output) const {
    output.writeByte(COMPLETE_ENCODING); // encoding version
    output.writeInt(num_vertices());
    output.writeBinaryString(vertices(), sizeof(*(vertices())) * num_vertices());
    output.writeBool(origin_inside());
    vassert(depth() >= 0);
    output.writeInt(depth());

    S2LatLngRect bound = GetRectBound();
    saveBoundToBuffer(&bound, output);
}


inline void Polygon::init(std::vector<std::unique_ptr<S2Loop> >* loops, bool doRepairs) {
    std::vector<S2Loop*> rawPtrVector;
    rawPtrVector.reserve(loops->size());
    for (int i = 0; i < loops->size(); ++i) {
        rawPtrVector.push_back(loops->at(i).release());
    }

    // base class method accepts a raw pointer vector,
    // and takes ownership of loops.
    Init(&rawPtrVector, doRepairs);
}

inline std::size_t Polygon::serializedLengthNoLoops() {
    return
        sizeof(int8_t) + // encoding version
        sizeof(int8_t) + // owns loops
        sizeof(int8_t) + // has holes
        sizeof(int32_t) + // num loops
        BOUND_SERIALIZED_SIZE;
}

inline std::size_t Polygon::serializedLength() const {
    std::size_t answer = serializedLengthNoLoops();
    const std::vector<S2Loop *> &theLoops = loops();
    for (int i = 0; i < theLoops.size(); i += 1) {
        answer += Loop::serializedLength(theLoops.at(i)->num_vertices());
    }
    return answer;
}

template<class Serializer, class Deserializer>
inline void Polygon::copyViaSerializers(Serializer& output, Deserializer& input) {
    int8_t version = input.readByte();

    if (version == COMPLETE_ENCODING) {
        output.writeByte(COMPLETE_ENCODING);
        output.writeByte(input.readByte()); // ownsLoops
        output.writeByte(input.readByte()); // has holes

        int32_t numLoops = input.readInt();
        output.writeInt(numLoops);
        for (int i = 0; i < numLoops; ++i) {
            Loop::copyViaSerializers(output, input);
        }

        copyBoundViaSerializers(output, input);
    } else {
        vassert(version == INCOMPLETE_ENCODING_FROM_JAVA);

        // This is a serialized polygon from Java, which won't have
        // proper bounding boxes defined.  Grab the vertices, build
        // the loops, and instantiate a polygon, which will
        // create the bounding boxes.

        input.readByte(); // owns loops
        input.readByte(); // has holes

        std::vector<std::unique_ptr<S2Loop> > loops;
        int32_t numLoops = input.readInt();
        loops.reserve(numLoops);
        for (int i = 0; i < numLoops; ++i) {
            std::vector<S2Point> points;
            Loop::pointArrayFromBuffer(input, &points);
            loops.push_back(std::unique_ptr<S2Loop>(new S2Loop()));
            loops.back()->Init(points);
        }

        skipBound(input);

        Polygon poly;
        // Don't do any orientation repairs here.
        poly.init(&loops, false);
        poly.saveToBuffer(output);
    }
}

static inline void DeleteLoopsInVector(vector<S2Loop*>* loops) {
  for (int i = 0; i < loops->size(); ++i) {
    delete loops->at(i);
  }
  loops->clear();
}

inline void Polygon::initFromGeography(const GeographyValue& geog, bool doRepairs) {
    ReferenceSerializeInputLE input(geog.data(), geog.length());
    initFromBuffer(input, doRepairs);
}

template<class Deserializer>
inline void Polygon::initFromBuffer(Deserializer& input, bool doRepairs) {
    input.readByte(); // encoding version

    if (owns_loops()) {
        DeleteLoopsInVector(&(loops()));
    }

    set_owns_loops(input.readBool());
    set_has_holes(input.readBool());
    int numLoops = input.readInt();

    loops().clear();
    loops().reserve(numLoops);
    int num_vertices = 0;
    for (int i = 0; i < numLoops; ++i) {
        Loop *loop = new Loop;
        loops().push_back(loop);
        loop->initFromBuffer(input, doRepairs);
        num_vertices += loop->num_vertices();
    }

    set_num_vertices(num_vertices);

    S2LatLngRect bound;
    initBoundFromBuffer(&bound, input);
    set_rect_bound(bound);
    // If we are asked to do repairs we want to
    // reinitialize the polygon.  The depths of
    // loops may have changed.
    if (doRepairs) {
        CalculateLoopDepths();
    }
}

template<class Serializer>
void Polygon::saveToBuffer(Serializer& output) const {
    output.writeByte(COMPLETE_ENCODING); // encoding version
    output.writeBool(owns_loops());
    output.writeBool(has_holes());
    output.writeInt(loops().size());
    for (int i = 0; i < num_loops(); ++i) {
        static_cast<const Loop*>(loop(i))->saveToBuffer(output);
    }

    S2LatLngRect bound = GetRectBound();
    saveBoundToBuffer(&bound, output);
}

} // end namespace voltdb

