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

#include <cassert>
#include <vector>

#include <boost/algorithm/string.hpp>
#include "boost/foreach.hpp"
#include <boost/geometry.hpp>
#include <boost/geometry/algorithms/append.hpp>
#include <boost/geometry/algorithms/area.hpp>
#include <boost/geometry/algorithms/correct.hpp>
#include <boost/geometry/algorithms/within.hpp>
#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/geometries/adapted/boost_tuple.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/multi/algorithms/append.hpp>
#include <boost/geometry/multi/geometries/multi_polygon.hpp>

#include "common/NValue.hpp"
#include "common/PlannerDomValue.h"
#include "common/ValuePeeker.hpp"
#include "expressions/geofunctions.h"

namespace voltdb {

    namespace bg = boost::geometry;

    typedef bg::cs::spherical_equatorial<bg::degree> CoordSys;

    // Points are defined using doubles
    typedef bg::model::d2::point_xy<double, CoordSys> Point;

    typedef bg::model::polygon<Point> Polygon;
    typedef bg::model::multi_polygon<Polygon> MultiPolygon;

    static void throwGeoJsonFormattingError(const std::string& msg) {
        char exMsg[1024];
        snprintf(exMsg, sizeof(exMsg), "Invalid GeoJSON: %s", msg.c_str());
        throw SQLException(SQLException::data_exception_invalid_parameter, exMsg);
    }

    static std::string getJsonGeometryType(const PlannerDomValue& pdv) {
        return pdv.valueForKey("type").asStr();
    }

    static Point geoJsonToPoint(const PlannerDomValue& root) {

        std::string geometryType = getJsonGeometryType(root);
        if (! boost::iequals("Point", geometryType)) {
            throwGeoJsonFormattingError("expected value of \"type\" to be \"Point\"");
        }

        PlannerDomValue coords = root.valueForKey("coordinates");
        double xCoord = coords.valueAtIndex(0).asDouble();
        double yCoord = coords.valueAtIndex(1).asDouble();

        return Point(xCoord, yCoord);
    }

    // static void debugPoly(const Polygon& poly) {
    //     std::cout << "Polygon: [\n";
    //     for (auto p : poly.outer()) {
    //         std::cout << "  (" << p.x() << ", " << p.y() << ")\n";
    //     }
    //     std::cout << "]\n";

    //     if (! poly.inners().empty()) {
    //         std::cout << "Inner rings:\n";
    //         for (auto inner : poly.inners()) {
    //             std::cout << "[\n";
    //             for (auto p : inner) {
    //                 std::cout << "  (" << p.x() << ", " << p.y() << ")\n";
    //             }
    //             std::cout << "[\n";
    //         }
    //     }
    // }

    Polygon getPolyFromRings(const PlannerDomValue& rings) {
        Polygon poly;
        int numRings = rings.arrayLen();
        assert(numRings >= 1); // for now only support one ring: the outer one.
        for (int ringIdx = 0; ringIdx < numRings; ++ringIdx) {

            if (ringIdx != 0) {
                poly.inners().push_back(Polygon::ring_type());
            }

            PlannerDomValue ring = rings.valueAtIndex(ringIdx);
            int numPoints = ring.arrayLen();
            for (int j = 0; j < numPoints; ++j) {
                double x = ring.valueAtIndex(j).valueAtIndex(0).asDouble();
                double y = ring.valueAtIndex(j).valueAtIndex(1).asDouble();
                if (ringIdx == 0) {
                    // the outer ring
                    bg::append(poly, Point(x, y));
                }
                else {
                    // an inner ring
                    bg::append(poly, Point(x, y), ringIdx - 1);
                }
            }
        }

        return poly;
    }

    static MultiPolygon geoJsonToMultiPolygon(const PlannerDomValue& root) {
        MultiPolygon multiPoly;

        std::string geometryType = getJsonGeometryType(root);
        if (boost::iequals(geometryType, "multipolygon")) {
            PlannerDomValue polys = root.valueForKey("coordinates");
            int numPolys = polys.arrayLen();
            for (int i = 0; i < numPolys; ++i) {

                PlannerDomValue rings = polys.valueAtIndex(i);
                multiPoly.push_back(getPolyFromRings(rings));
            }
        }
        else {
            assert (boost::iequals(geometryType, "polygon"));
            PlannerDomValue rings = root.valueForKey("coordinates");
            multiPoly.push_back(getPolyFromRings(rings));
        }

        // for (auto poly : multiPoly) {
        //     debugPoly(poly);
        // }

        bg::correct(multiPoly);

        return multiPoly;
    }

    template<class Geom>
    static void geoJsonToGeometryHelper(const PlannerDomValue &pdv, Geom& geom);

    template<>
    void geoJsonToGeometryHelper<Point>(const PlannerDomValue& pdv, Point &pt) {
        pt = geoJsonToPoint(pdv);
    }

    template<>
    void geoJsonToGeometryHelper<MultiPolygon>(const PlannerDomValue& pdv, MultiPolygon &pt) {
        pt = geoJsonToMultiPolygon(pdv);
    }

    template<class Geom>
    static void geoJsonToGeometry(const NValue& nval, Geom &geom) {
        void* voidData = ValuePeeker::peekObjectValue_withoutNull(nval);
        const char* charData = static_cast<char*>(voidData);
        int32_t len = ValuePeeker::peekObjectLength_withoutNull(nval);

        // This guarantees the data is null-terminated...
        std::string strData(charData, len);

        PlannerDomRoot pdr(strData.c_str());
        PlannerDomValue pdv = pdr.rootObject();
        geoJsonToGeometryHelper(pdv, geom);
    }

    static bool anyNulls(const std::vector<NValue>& args) {
        BOOST_FOREACH(const NValue& n,  args) {
            if (n.isNull()) {
                return true;
            }
        }
        return false;
    }

    static ValueType checkVarcharArgs(const std::vector<NValue>& args) {
        BOOST_FOREACH(const NValue& n,  args) {
            ValueType vt = ValuePeeker::peekValueType(n);
            if (vt != VALUE_TYPE_VARCHAR) {
                return vt;
            }
        }
        return VALUE_TYPE_VARCHAR;
    }

    template<> NValue NValue::call<FUNC_VOLT_GEO_WITHIN>(const std::vector<NValue>& arguments) {
        ValueType vt = checkVarcharArgs(arguments);
        if (vt != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(vt, VALUE_TYPE_VARCHAR);
        }

        if (anyNulls(arguments)) {
            return NValue::getNullValue(VALUE_TYPE_INTEGER);
        }

        Point pt;
        geoJsonToGeometry(arguments[0], pt);

        MultiPolygon multiPoly;
        geoJsonToGeometry(arguments[1], multiPoly);

        bool b = bg::within(pt, multiPoly);

        NValue result(VALUE_TYPE_INTEGER);
        result.getInteger() = b ? 1 : 0;
        return result;
    }

    template<> NValue NValue::callUnary<FUNC_VOLT_GEO_AREA>() const {
        if (getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(getValueType(), VALUE_TYPE_VARCHAR);
        }

        if (isNull())
            return NValue::getNullValue(VALUE_TYPE_DOUBLE);

        MultiPolygon multiPoly;
        geoJsonToGeometry(*this, multiPoly);

        double area = bg::area(multiPoly);
        NValue result(VALUE_TYPE_DOUBLE);
        result.getDouble() = area;

        return result;
    }

    template<> NValue NValue::call<FUNC_VOLT_GEO_DISTANCE>(const std::vector<NValue>& arguments) {
        ValueType vt = checkVarcharArgs(arguments);
        if (vt != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(vt, VALUE_TYPE_VARCHAR);
        }

        if (anyNulls(arguments)) {
            return NValue::getNullValue(VALUE_TYPE_DOUBLE);
        }

        Point p1;
        geoJsonToGeometry(arguments[0], p1);

        Point p2;
        geoJsonToGeometry(arguments[1], p2);

        double dist = bg::distance(p1, p2);
        NValue result(VALUE_TYPE_DOUBLE);
        result.getDouble() = dist;
        return result;
    }

    template<> NValue NValue::callUnary<FUNC_VOLT_GEO_NUM_POLYGONS>() const {
        if (getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(getValueType(), VALUE_TYPE_VARCHAR);
        }

        if (isNull())
            return NValue::getNullValue(VALUE_TYPE_BIGINT);

        MultiPolygon multiPoly;
        geoJsonToGeometry(*this, multiPoly);

        std::size_t numGeoms = bg::num_geometries(multiPoly);
        NValue result(VALUE_TYPE_BIGINT);
        result.getBigInt() = numGeoms;

        return result;
    }

    template<> NValue NValue::callUnary<FUNC_VOLT_GEO_NUM_INTERIOR_RINGS>() const {
        if (getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(getValueType(), VALUE_TYPE_VARCHAR);
        }

        if (isNull())
            return NValue::getNullValue(VALUE_TYPE_BIGINT);

        MultiPolygon multiPoly;
        geoJsonToGeometry(*this, multiPoly);

        std::size_t numInteriorRings = bg::num_interior_rings(multiPoly);
        NValue result(VALUE_TYPE_BIGINT);
        result.getBigInt() = numInteriorRings;

        return result;
    }

    template<> NValue NValue::callUnary<FUNC_VOLT_GEO_NUM_POINTS>() const {
        if (getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(getValueType(), VALUE_TYPE_VARCHAR);
        }

        if (isNull())
            return NValue::getNullValue(VALUE_TYPE_BIGINT);

        MultiPolygon multiPoly;
        geoJsonToGeometry(*this, multiPoly);

        std::size_t numPoints = bg::num_points(multiPoly);
        NValue result(VALUE_TYPE_BIGINT);
        result.getBigInt() = numPoints;

        return result;
    }

    template<> NValue NValue::callUnary<FUNC_VOLT_GEO_PERIMETER>() const {
        if (getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException(getValueType(), VALUE_TYPE_VARCHAR);
        }

        if (isNull())
            return NValue::getNullValue(VALUE_TYPE_DOUBLE);

        MultiPolygon multiPoly;
        geoJsonToGeometry(*this, multiPoly);

        // perimeter produces a "long double" hence the cast.
        double perim = static_cast<double>(bg::perimeter(multiPoly));
        NValue result(VALUE_TYPE_DOUBLE);
        result.getDouble() = perim;

        return result;
    }

} // end namespace voltdb
