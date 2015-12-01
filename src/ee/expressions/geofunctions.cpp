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

#include <memory>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>

#include "common/ValueFactory.hpp"
#include "expressions/geofunctions.h"

#include "s2geo/s2latlng.h"

namespace voltdb {

static const int POINT = FUNC_VOLT_POINTFROMTEXT;
static const int POLY = FUNC_VOLT_POLYGONFROMTEXT;

static const double EARTH_AREA_SQ_M = 510.072E12;
static const double EARTH_RADIUS_METERS = 6371000;

typedef boost::tokenizer<boost::char_separator<char> > Tokenizer;

static void throwInvalidWktPoint(const std::string& input)
{
    std::ostringstream oss;
    oss << "Invalid input to POINTFROMTEXT: ";
    oss << "'" << input << "', ";
    oss << "expected input of the form 'POINT(<lat> <lng>)'";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidWktPoly(const std::string& reason)
{
    std::ostringstream oss;
    oss << "Invalid input to POLYGONFROMTEXT: " << reason << ".  ";
    oss << "Expected input of the form 'POLYGON((<lat> <lng>, ...), ...)'";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static GeographyPointValue::Coord stringToCoord(int pointOrPoly,
                                  const std::string& input,
                           const std::string& val)
{
    GeographyPointValue::Coord coord = 0.0;
    try {
        coord = boost::lexical_cast<GeographyPointValue::Coord>(val);
    }
    catch (const boost::bad_lexical_cast&) {
        if (pointOrPoly == POLY) {
            throwInvalidWktPoly("expected a number but found '" + val + "'");
        }
        else {
            throwInvalidWktPoint(input);
        }
    }

    return coord;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINTFROMTEXT>() const
{
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_POINT);
    }

    std::string wkt(reinterpret_cast<char*>(getObjectValue_withoutNull()),
                          getObjectLength_withoutNull());

    // Discard whitespace, but return commas or parentheses as tokens
    Tokenizer tokens(wkt, boost::char_separator<char>(" \f\n\r\t\v", ",()"));
    Tokenizer::iterator it = tokens.begin();
    Tokenizer::iterator end = tokens.end();

    if (! boost::iequals(*it, "point")) {
        throwInvalidWktPoint(wkt);
    }
    ++it;

    if (! boost::iequals(*it, "(")) {
        throwInvalidWktPoint(wkt);
    }
    ++it;


    GeographyPointValue::Coord lat = stringToCoord(POINT, wkt, *it);
    ++it;

    GeographyPointValue::Coord lng = stringToCoord(POINT, wkt, *it);
    ++it;

    if (! boost::iequals(*it, ")")) {
        throwInvalidWktPoint(wkt);
    }
    ++it;

    if (it != end) {
        throwInvalidWktPoint(wkt);
    }

    NValue returnValue(VALUE_TYPE_POINT);
    returnValue.getPoint() = GeographyPointValue(lat, lng);

    return returnValue;
}

static void readLoop(const std::string &wkt,
                     Tokenizer::iterator &it,
                     const Tokenizer::iterator &end,
                     S2Loop *loop)
{
    if (! boost::iequals(*it, "(")) {
        throwInvalidWktPoly("expected left parenthesis to start a loop");
    }
    ++it;

    std::vector<S2Point> points;
    while (it != end && *it != ")") {
        GeographyPointValue::Coord lat = stringToCoord(POLY, wkt, *it);
        ++it;

        GeographyPointValue::Coord lng = stringToCoord(POLY, wkt, *it);
        ++it;

        points.push_back(S2LatLng::FromDegrees(lat, lng).ToPoint());

        if (*it == ",") {
            ++it;
            // continue to next lat long pair
        }
        else if (*it != ")") {
            throwInvalidWktPoly("unexpected token: '" + (*it) + "'");
        }
    }

    if (it == end) {
        // we hit the end of input before the closing parenthesis
        throwInvalidWktPoly("unexpected end of input");
    }

    assert (*it == ")");

    // Advance iterator to next token
    ++it;

    if (points.size() < 4) {
        throwInvalidWktPoly("A polygon ring must contain at least 4 points (including repeated closing vertex)");
    }

    const S2Point& first = points.at(0);
    const S2Point& last = points.at(points.size() - 1);

    if (first != last) {
        throwInvalidWktPoly("A polygon ring's first vertex must be equal to its last vertex");
    }

    // S2 format considers the closing vertex in a loop to be
    // implicit, while in WKT it is explicit.  Remove the closing
    // vertex here to reflect this.
    points.pop_back();

    loop->Init(points);
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGONFROMTEXT>() const
{
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_GEOGRAPHY);
    }

    const std::string wkt(reinterpret_cast<char*>(getObjectValue_withoutNull()),
                          getObjectLength_withoutNull());

    // Discard whitespace, but return commas or parentheses as tokens
    Tokenizer tokens(wkt, boost::char_separator<char>(" \f\n\r\t\v", ",()"));
    Tokenizer::iterator it = tokens.begin();
    Tokenizer::iterator end = tokens.end();

    if (! boost::iequals(*it, "polygon")) {
        throwInvalidWktPoly("does not start with POLYGON keyword");
    }
    ++it;

    if (! boost::iequals(*it, "(")) {
        throwInvalidWktPoly("missing left parenthesis after POLYGON keyword");
    }
    ++it;

    std::size_t length = Polygon::serializedLengthNoLoops();
    std::vector<std::unique_ptr<S2Loop> > loops;
    while (it != end) {
        loops.push_back(std::unique_ptr<S2Loop>(new S2Loop()));
        readLoop(wkt, it, end, loops.back().get());
        length += Loop::serializedLength(loops.back()->num_vertices());
        if (*it == ",") {
            ++it;
        }
        else if (*it == ")") {
            ++it;
            break;
        }
        else {
            throwInvalidWktPoly("unexpected token: '" + (*it) + "'");
        }
    }

    if (it != end) {
        // extra stuff after input
        throwInvalidWktPoly("unrecognized input after WKT: '" + (*it) + "'");
    }

    NValue nval = ValueFactory::getUninitializedTempGeographyValue(length);
    char* storage = static_cast<char*>(ValuePeeker::peekObjectValue_withoutNull(nval));

    Polygon poly;
    poly.init(&loops); // polygon takes ownership of loops here.
    SimpleOutputSerializer output(storage, length);
    poly.saveToBuffer(output);
    return nval;
}

template<> NValue NValue::call<FUNC_VOLT_CONTAINS>(const std::vector<NValue>& arguments) {
    if (arguments[0].isNull() || arguments[1].isNull())
        return NValue::getNullValue(VALUE_TYPE_BOOLEAN);

    Polygon poly;
    poly.initFromGeography(arguments[0].getGeography());
    S2Point pt = arguments[1].getPoint().toS2Point();
    return ValueFactory::getBooleanValue(poly.Contains(pt));
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS>() const {
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_INTEGER);
    }

    Polygon poly;
    poly.initFromGeography(getGeography());

    NValue retVal(VALUE_TYPE_INTEGER);
    // exclude exterior ring
    retVal.getInteger() = poly.num_loops() - 1;
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_NUM_POINTS>() const {
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_INTEGER);
    }

    Polygon poly;
    poly.initFromGeography(getGeography());

    // the OGC spec suggests that the number of vertices should
    // include the repeated closing vertex which is implicit in S2's
    // representation.  So add an extra vertex for each loop.
    int32_t numPoints = poly.num_vertices() + poly.num_loops();

    NValue retVal(VALUE_TYPE_INTEGER);
    retVal.getInteger() = numPoints;
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINT_LATITUDE>() const {
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_DOUBLE);
    }
    const GeographyPointValue point = getPoint();
    NValue retVal(VALUE_TYPE_DOUBLE);
    retVal.getDouble() = point.getLatitude();
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINT_LONGITUDE>() const {
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_DOUBLE);
    }
    const GeographyPointValue point = getPoint();
    NValue retVal(VALUE_TYPE_DOUBLE);
    retVal.getDouble() = point.getLongitude();
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_CENTROID>() const {
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_POINT);
    }

    Polygon polygon;
    polygon.initFromGeography(getGeography());
    const GeographyPointValue point(polygon.GetCentroid());
    NValue retVal(VALUE_TYPE_POINT);
    retVal.getPoint() = point;
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_AREA>() const {
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_DOUBLE);
    }

    Polygon polygon;
    polygon.initFromGeography(getGeography());

    NValue retVal(VALUE_TYPE_DOUBLE);
    // area is in steradians which is a solid angle. Earth in the calculation is treated as sphere
    // and a complete sphere subtends 4π steradians (https://en.wikipedia.org/wiki/Steradian).
    // Taking area of earth as 510.072*10^12 sq meters,area for the given steradians can be calculated as:
    retVal.getDouble() = polygon.GetArea() * EARTH_AREA_SQ_M / (4 * M_PI);
    return retVal;
}

template<> NValue NValue::call<FUNC_VOLT_DISTANCE_POLYGON_POINT>(const std::vector<NValue>& arguments) {
    assert(arguments[0].getValueType() == VALUE_TYPE_GEOGRAPHY);
    assert(arguments[1].getValueType() == VALUE_TYPE_POINT);

    if (arguments[0].isNull() || arguments[1].isNull()) {
        return NValue::getNullValue(VALUE_TYPE_DOUBLE);
    }

    Polygon polygon;
    polygon.initFromGeography(arguments[0].getGeography());
    GeographyPointValue point = arguments[1].getPoint();
    NValue retVal(VALUE_TYPE_DOUBLE);
    // distance is in radians, so convert it to meters
    retVal.getDouble() = polygon.getDistance(point) * EARTH_RADIUS_METERS;
    return retVal;
}

template<> NValue NValue::call<FUNC_VOLT_DISTANCE_POINT_POINT>(const std::vector<NValue>& arguments) {
    assert(arguments[0].getValueType() == VALUE_TYPE_POINT);
    assert(arguments[1].getValueType() == VALUE_TYPE_POINT);

    if (arguments[0].isNull() || arguments[1].isNull()) {
        return NValue::getNullValue(VALUE_TYPE_DOUBLE);
    }

    // compute distance using Haversine formula
    // alternate to this is just obtain 2 s2points and compute S1Angle between them
    // and use that as distance.
    // S2 test uses S2LatLng for computing distances
    const S2LatLng latLng1 = S2LatLng(arguments[0].getPoint().toS2Point()).Normalized();
    const S2LatLng latLng2 = S2LatLng(arguments[1].getPoint().toS2Point()).Normalized();
    S1Angle distance = latLng1.GetDistance(latLng2);
    NValue retVal(VALUE_TYPE_DOUBLE);
    // distance is in radians, so convert it to meters
    retVal.getDouble() = distance.radians() * EARTH_RADIUS_METERS;
    return retVal;
}

} // end namespace voltdb
