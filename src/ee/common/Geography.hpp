/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef EE_COMMON_GEOGRAPHY_HPP
#define EE_COMMON_GEOGRAPHY_HPP

#include <sstream>
#include <vector>

#include "boost/foreach.hpp"
#include "boost/functional/hash.hpp"

#include "common/value_defs.h"
#include "common/Point.hpp"

#include "s2geo/s2loop.h"
#include "s2geo/s2polygon.h"

namespace voltdb {

/**
 * A class for representing loops of a polygon.  A polygon is composed
 * of one or more loops, where the first loop delineates area inside
 * the polygon, and subsequent loops delineate negative areas or
 * "holes."
 *
 * This class can be treated as a container for the vertexes in the
 * loop, and used with a range-based for loop:
 *
 *   Loop loop = ...;
 *   BOOST_FOREACH (const Point& vertex, loop) {
 *       // ...
 *   }
 *
 *   [Or, with C++11...]
 *
 *   Loop loop = ...;
 *   for (const Point& pt : loop) {
 *     ...
 *   }
 */
class Loop {
public:

    // BOOST_FOREACH needs these types to work.
    typedef const Point* iterator;
    typedef const Point* const_iterator;

    explicit Loop(const char* data) : m_data(data) {}

    int32_t numVertices() const {
        return reinterpret_cast<const int32_t*>(m_data)[0];
    }

    const Point* begin() const {
        const char* pos = m_data + sizeof(int32_t);
        return reinterpret_cast<const Point*>(pos);
    }

    const Point* end() const {
        const char* pos = m_data + sizeof(int32_t) + sizeof(Point) * numVertices();
        return reinterpret_cast<const Point*>(pos);
    }

private:
    const char * const m_data;
};

/**
 * A class for representing the collection of loops that make up a
 * polygon.  When compiling with C++11 support (soon?), this class can
 * be used in a range-based for loop to iterate over each loop:
 *
 *   LoopContainer lc = geog.loops();
 *   for (Loop loop : lc) {
 *     ...
 *   }
 *
 * It should be possible to get this class to work with BOOST_FOREACH,
 * but there's some tricky template trickery required to get that to
 * work.
 */
class LoopContainer {
public:

    class iterator {
    public:

        iterator(const char* pos, std::size_t numRemainingLoops)
            : m_pos(pos)
            , m_numRemainingLoops(numRemainingLoops)
        {
        }

        // this constructor is used to create the end iterator
        iterator()
            : m_pos(NULL)
            , m_numRemainingLoops(0)
        {
        }

        // pre-increment
        iterator& operator++() {
            --m_numRemainingLoops;
            if (m_numRemainingLoops == 0) {
                // the end iterator.
                m_pos = 0;
                return *this;
            }

            int32_t numVerts = Loop(m_pos).numVertices();
            m_pos += sizeof(int32_t) + sizeof(Point) * numVerts;
            return *this;
        }

        const Loop operator*() const {
            return Loop(m_pos);
        }

        bool operator!=(const iterator& that) const {
            return m_pos != that.m_pos;
        }

        bool operator==(const iterator& that) const {
            return m_pos == that.m_pos;
        }

    private:
        const char* m_pos;
        std::size_t m_numRemainingLoops;
    };

    LoopContainer(const char* data)
        : m_data(data)
    {
    }

    // return the number of loops
    // (using std::size_t here to follow convention of std containers)
    std::size_t size() const {
        return static_cast<const std::size_t>(reinterpret_cast<const int32_t*>(m_data)[0]);
    }

    iterator begin() const {
        return iterator(m_data + sizeof(int32_t), size());
    }

    iterator end() const {
        return iterator();
    }

private:
    const char* const m_data;
};

/**
 * A class for representing instances of geo-spatial geographies.
 * (Currently only polygons can be represented here.)
 *
 * Accepts a pointer to a variable-length byte sequence
 * [4 bytes] number of loops prefix
 * and for each loop:
 *   [4 bytes] number of vertices
 *   and for each vertex:
 *     a serialization of the Point
 *
 * Note that variable length data in the EE is typically prefixed with a length.
 * The pointer here should be to the start of the data just after the length.
 */
class Geography {
public:

    /** Constructor for a null geography */
    Geography()
        : m_data(NULL)
    {
    }

    /**
     * Create a polygon from a variable-length byte sequence.  This
     * object does not own the data and should not free it---the data
     * pointer points into a buffer that is managed in the same manner
     * as VARCHAR and VARBINARY data.
     */
    Geography(void* data)
        : m_data(static_cast<char*>(data))
    {
        assert (m_data != NULL);
    }

    bool isNull() const {
        return m_data == NULL;
    }

    int32_t numLoops() const {
        if (isNull())
            return 0;

        return reinterpret_cast<const int32_t*>(m_data)[0];
    }

    // number of points/vertices polygon has. It is sum of
    // number of points it's loops has.
    int32_t numVertices() const {
        LoopContainer::iterator it = loops().begin();
        LoopContainer::iterator loopsEnd = loops().end();
        int32_t numPoints = 0;
        for (; it != loopsEnd; ++it) {
            numPoints = numPoints + (*it).numVertices();
        }
        return numPoints;
    }

    LoopContainer loops() const {
        assert(! isNull());
        return LoopContainer(m_data);
    }

    std::unique_ptr<S2Polygon> toS2Polygon() const {
        std::vector<S2Loop*> loops;
        auto loopIt = this->loops().begin();
        auto loopEnd = this->loops().end();
        for (; loopIt != loopEnd; ++loopIt) {
            std::vector<S2Point> verts;
            auto vertIt = (*loopIt).begin();
            // S2 considers the final closing vertex
            // (which should be identical to the first vertex)
            // to be implicit.
            auto vertEnd = (*loopIt).end() - 1;
            for (; vertIt != vertEnd; ++vertIt) {
                verts.push_back(vertIt->toS2Point());
            }
            S2Loop* loop = new S2Loop();
            loop->Init(verts);
            loops.push_back(loop);
        }

        std::unique_ptr<S2Polygon> s2Poly(new S2Polygon());

        // The polygon will take ownership of the loops here.
        s2Poly->Init(&loops);
        assert(loops.empty());

        return s2Poly;
    }

    /**
     * Do a comparison with another geography (polygon).
     *
     * Let's do floating-point comparisons only as a last resort to
     * help avoid issues with floating-point math.  It doesn't really
     * matter how we do our comparison as long as we produce a
     * deterministic order.
     *
     *   1. First compare number of loops (polygons with fewer loops sort as smaller).
     *   2. If the number of loops are the same, compare on the number of vertices
     *        in the loops.  The polygon with fewer vertices will sort as smaller.
     *   3. Finally, if all loops have the same number of vertices, sort on the
     *        points themselves (which will involve doing floating-point comparison)
     */
    int compareWith(const Geography& rhs) const {
        assert(! isNull() && !rhs.isNull());

        if (numLoops() < rhs.numLoops()) {
            return VALUE_COMPARE_LESSTHAN;
        }

        if (numLoops() > rhs.numLoops()) {
            return VALUE_COMPARE_GREATERTHAN;
        }

        // number of loops are the same.
        // compare on number of vertices in each loop
        LoopContainer::iterator rhsLoopIt = rhs.loops().begin();
        LoopContainer::iterator lhsLoopIt = loops().begin();
        LoopContainer::iterator lhsLoopEnd = loops().end();
        for (; lhsLoopIt != lhsLoopEnd; ++lhsLoopIt) {
             Loop loop = *lhsLoopIt;
             if (loop.numVertices() < (*rhsLoopIt).numVertices()) {
                 return VALUE_COMPARE_LESSTHAN;
             }
             else if (loop.numVertices() > (*rhsLoopIt).numVertices()) {
                 return VALUE_COMPARE_GREATERTHAN;
             }
             ++rhsLoopIt;
        }

        // Each loop has the same number of vertices.
        // Compare the vertices themselves.
        rhsLoopIt = rhs.loops().begin();
        lhsLoopIt = loops().begin();
        for (; lhsLoopIt != lhsLoopEnd; ++lhsLoopIt) {
            Loop loop = *lhsLoopIt;
            Loop::iterator rhsVertexIt = (*rhsLoopIt).begin();
            BOOST_FOREACH(const Point &vertex, loop) {
                int cmpResult = vertex.compareWith(*rhsVertexIt);
                if (cmpResult != VALUE_COMPARE_EQUAL) {
                    return cmpResult;
                }

                ++rhsVertexIt;
            }
            ++rhsLoopIt;
        }

        return VALUE_COMPARE_EQUAL;
    }

    /**
     * Serialize this geography
     */
    template<class Serializer>
    void serializeTo(Serializer& output) const {
        assert (! isNull());
        int32_t nLoops = numLoops();
        assert(nLoops > 0);
        output.writeInt(nLoops);

        LoopContainer::iterator it = loops().begin();
        LoopContainer::iterator loopsEnd = loops().end();
        for (; it != loopsEnd; ++it) {
            Loop loop = *it;
            int numVerts = loop.numVertices();
            assert(numVerts > 0);
            output.writeInt(numVerts);
            BOOST_FOREACH(const Point& vertex, loop) {
                vertex.serializeTo(output);
            }
        }
    }

    /**
     * Populate a pointer to storage with the bytes that represent a
     * geography.  Note that the caller has already read the
     * length-prefix that accompanies variable-length data and sized
     * the target storage appropriately.
     */
    template<class Deserializer>
    static void deserializeFrom(Deserializer& input,
                                char* storage,
                                int32_t length)
    {
        char *pos = storage;
        int32_t nLoops = input.readInt();
        assert(nLoops > 0);
        reinterpret_cast<int32_t*>(pos)[0] = nLoops;
        pos += sizeof(nLoops);

        for (int i = 0; i < nLoops; ++i) {

            int32_t numVertices = input.readInt();
            reinterpret_cast<int32_t*>(pos)[0] = numVertices;
            pos += sizeof(numVertices);

            for (int j = 0; j < numVertices; ++j) {
                const Point pt = Point::deserializeFrom(input);
                reinterpret_cast<Point::Coord*>(pos)[0] = pt.getLatitude();
                reinterpret_cast<Point::Coord*>(pos)[1] = pt.getLongitude();
                pos += sizeof(Point::Coord) * 2;
            }
        }
    }

    void hashCombine(std::size_t& seed) const {

        if (isNull()) {
            // Treat a null as a polygon with zero loops
            boost::hash_combine(seed, 0);
            return;
        }

        boost::hash_combine(seed, numLoops());
        LoopContainer::iterator it = loops().begin();
        LoopContainer::iterator loopsEnd = loops().end();
        for (; it != loopsEnd; ++it) {
            Loop loop = *it;
            boost::hash_combine(seed, loop.numVertices());
            BOOST_FOREACH(const Point& pt, loop) {
                pt.hashCombine(seed);
            }
        }
    }

    std::string toString() const {
        std::ostringstream oss;

        if (isNull()) {
            oss << "null polygon";
        }
        else {
            oss << "polygon with ";
            oss << numLoops() << " loops with vertex counts";
            LoopContainer::iterator it = loops().begin();
            LoopContainer::iterator loopsEnd = loops().end();
            for (; it != loopsEnd; ++it) {
                Loop loop = *it;
                oss << " " << loop.numVertices();
            }
        }

        return oss.str();
    }

private:
    const char* const m_data;
};

} // end namespace

#endif // EE_COMMON_GEOGRAPHY_HPP
