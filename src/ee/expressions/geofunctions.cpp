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

#include <memory>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>

#include "common/ValueFactory.hpp"
#include "expressions/geofunctions.h"

namespace voltdb {

static const int POINT = FUNC_VOLT_POINTFROMTEXT;
static const int POLY = FUNC_VOLT_POLYGONFROMTEXT;

static const double SPHERICAL_EARTH_MEAN_RADIUS_M = 6371008.8; // mean radius in meteres
static const double RADIUS_SQ_M = SPHERICAL_EARTH_MEAN_RADIUS_M * SPHERICAL_EARTH_MEAN_RADIUS_M;

typedef boost::tokenizer<boost::char_separator<char> > Tokenizer;

static bool isMultiPolygon(const Polygon &poly, std::stringstream *msg);
static void throwInvalidWktPoint(const std::string& input) {
    std::ostringstream oss;
    oss << "Invalid input to POINTFROMTEXT: ";
    oss << "'" << input << "', ";
    oss << "expected input of the form 'POINT(<lng> <lat>)'";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidWktPoly(const std::string& reason) {
    std::ostringstream oss;
    oss << "Invalid input to POLYGONFROMTEXT: " << reason << ".  ";
    oss << "Expected input of the form 'POLYGON((<lng> <lat>, ...), ...)'";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidMakeValidPoly(const std::string& reason) {
    std::ostringstream oss;
    oss << "Invalid input to MAKE_VALID_POLYGON: " << reason << ".";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidPointLatitude(const std::string& input) {
    std::ostringstream oss;
    oss << "Invalid input to POINTFROMTEXT: '" << input << "'";
    oss << ".  Latitude must be in the range [-90,90].";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidPointLongitude(const std::string& input) {
    std::ostringstream oss;
    oss << "Invalid input to POINTFROMTEXT: '" << input << "'";
    oss << ".  Longitude must be in the range [-180,180].";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidPolygonLatitude(const std::string& input) {
    std::ostringstream oss;
    oss << "Invalid input to POLYGONFROMTEXT: '" << input << "'";
    oss << ".  Latitude must be in the range [-90,90].";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidPolygonLongitude(const std::string& input) {
    std::ostringstream oss;
    oss << "Invalid input to POLYGONFROMTEXT: '" << input << "'";
    oss << ".  Longitude must be in the range [-180,180].";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static void throwInvalidDistanceDWithin(const std::string& msg) {
    std::ostringstream oss;
    oss << "Invalid input to DWITHIN function: '" << msg << "'.";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}


static GeographyPointValue::Coord stringToCoord(int pointOrPoly,
        const std::string& input, const std::string& val) {
    GeographyPointValue::Coord coord = 0.0;
    try {
        coord = boost::lexical_cast<GeographyPointValue::Coord>(val);
    } catch (const boost::bad_lexical_cast&) {
        if (pointOrPoly == POLY) {
            throwInvalidWktPoly("expected a number but found '" + val + "'");
        } else {
            throwInvalidWktPoint(input);
        }
    }

    return coord;
}


// function computes distance between two non-null points
// function computes distance using Haversine formula
static double getDistance(const GeographyPointValue &point1,
                          const GeographyPointValue &point2) {
    vassert(!point1.isNull());
    vassert(!point2.isNull());

    const S2LatLng latLng1 = S2LatLng(point1.toS2Point()).Normalized();
    const S2LatLng latLng2 = S2LatLng(point2.toS2Point()).Normalized();
    S1Angle distance = latLng1.GetDistance(latLng2);
    return distance.radians() * SPHERICAL_EARTH_MEAN_RADIUS_M;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINTFROMTEXT>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tPOINT);
    }

    int32_t textLength;
    const char* textData = getObject_withoutNull(textLength);
    std::string wkt(textData, textLength);

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


    GeographyPointValue::Coord lng = stringToCoord(POINT, wkt, *it);
    ++it;

    GeographyPointValue::Coord lat = stringToCoord(POINT, wkt, *it);
    ++it;

    if ( lng < -180.0 || lng > 180.0) {
        throwInvalidPointLongitude(wkt);
    } else if (lat < -90.0 || lat > 90.0 ) {
        throwInvalidPointLatitude(wkt);
    }

    if (! boost::iequals(*it, ")")) {
        throwInvalidWktPoint(wkt);
    }
    ++it;

    if (it != end) {
        throwInvalidWktPoint(wkt);
    }

    NValue returnValue(ValueType::tPOINT);
    returnValue.getGeographyPointValue() = GeographyPointValue(lng, lat);

    return returnValue;
}

#undef DEBUG_POLYGONS

#if defined(DEBUG_POLYGONS)
static void printLoop(int lidx, bool is_shell, S2Loop *loop) {
    std::cout << "Loop " << lidx << ": ";
    std::cout << (is_shell ? "" : "not ") << "a shell, ";
    std::cout << "depth = " << loop->depth();
    std::cout << ", is_hole = " << loop->is_hole();
    std::cout << ", points: ";
    std::string sep("");
    for (int idx = 0; idx < loop->num_vertices(); idx += 1) {
        S2LatLng ll(loop->vertex(idx));
        std::cout << sep << "(" << ll.lng().degrees() << ", " << ll.lat().degrees() << ")";
        sep = ", ";
    }
    std::cout << "\n";
}

static void printPolygon(const std::string &label, const S2Polygon *poly) {
    std::cout << label << ":\n";
    std::cout << (poly->has_holes() ? "Has holes" : "Has no holes") << std::endl;
    for (int lidx = 0; lidx < poly->num_loops(); lidx += 1) {
        S2Loop *loop = poly->loop(lidx);
        printLoop(lidx, !loop->is_hole(), loop);
    }
    std::cout << std::flush;
}
#else
#define printLoop(lidx, is_shell, loop)
#define printPolygon(label, poly)
#endif

static void readLoop(bool is_shell,
        const std::string &wkt,
        Tokenizer::iterator &it,
        const Tokenizer::iterator &end,
        S2Loop *loop) {
    if (! boost::iequals(*it, "(")) {
        throwInvalidWktPoly("expected left parenthesis to start a ring");
    }
    ++it;

    std::vector<S2Point> points;
    while (it != end && *it != ")") {
        GeographyPointValue::Coord lng = stringToCoord(POLY, wkt, *it);

        if (lng < -180 || lng > 180) {
            throwInvalidPolygonLongitude(*it);
        }
        ++it;
        GeographyPointValue::Coord lat = stringToCoord(POLY, wkt, *it);
        if (lat < -90 || lat > 90) {
            throwInvalidPolygonLatitude(*it);
        }
        ++it;

        // Note: This is S2.  It takes latitude, longitude, not
        //       longitude, latitude.
        points.push_back(S2LatLng::FromDegrees(lat, lng).ToPoint());

        if (*it == ",") {
            ++it;
            // continue to next lat long pair
        } else if (*it != ")") {
            throwInvalidWktPoly("unexpected token: '" + (*it) + "'");
        }
    }

    if (it == end) {
        // we hit the end of input before the closing parenthesis
        throwInvalidWktPoly("unexpected end of input");
    }

    vassert(*it == ")");

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
    // The first is a shell.  All others are holes.  We need to reverse
    // the order of the vertices for holes.
    if (!is_shell) {
        // Don't touch the first point.  We don't want to
        // cycle the vertices.
        std::reverse(++(points.begin()), points.end());
    }
    loop->Init(points);
}

static NValue polygonFromText(const std::string &wkt, bool doRepairs) {
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

    bool is_shell = true;
    // This is the length of the polygon when serialized.
    // We could get this with Polygon::serializedLenth.  But
    // that would require traversing the loops twice, and
    // who has time for that?
    std::size_t length = Polygon::serializedLengthNoLoops();
    std::vector<std::unique_ptr<S2Loop> > loops;
    while (it != end) {
        loops.push_back(std::unique_ptr<S2Loop>(new S2Loop()));
        readLoop(is_shell, wkt, it, end, loops.back().get());
        // Only the first loop is a shell.
        is_shell = false;
        length += Loop::serializedLength(loops.back()->num_vertices());
        if (*it == ",") {
            ++it;
        } else if (*it == ")") {
            ++it;
            break;
        } else {
            throwInvalidWktPoly("unexpected token: '" + (*it) + "'");
        }
    }

    if (it != end) {
        // extra stuff after input
        throwInvalidWktPoly("unrecognized input after WKT: '" + (*it) + "'");
    }

    NValue nval = ValueFactory::getUninitializedTempGeographyValue(length);
    char* storage = const_cast<char*>(ValuePeeker::peekObjectValue(nval));

    Polygon poly;
    poly.init(&loops, doRepairs); // polygon takes ownership of loops here.
    if (doRepairs) {
        std::stringstream validReason;
        if (!poly.IsValid(&validReason) || isMultiPolygon(poly, &validReason)) {
            throwInvalidWktPoly(validReason.str());
        }
    }
    SimpleOutputSerializer output(storage, length);
    poly.saveToBuffer(output);
    return nval;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGONFROMTEXT>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tGEOGRAPHY);
    }

    int32_t textLength;
    const char* textData = getObject_withoutNull(textLength);
    const std::string wkt(textData, textLength);

    return polygonFromText(wkt, false);
}

template<> NValue NValue::callUnary<FUNC_VOLT_VALIDPOLYGONFROMTEXT>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tGEOGRAPHY);
    }

    int32_t textLength;
    const char* textData = getObject_withoutNull(textLength);
    const std::string wkt(textData, textLength);

    return polygonFromText(wkt, true);
}

template<> NValue NValue::call<FUNC_VOLT_CONTAINS>(const std::vector<NValue>& arguments) {
    if (arguments[0].isNull() || arguments[1].isNull())
        return NValue::getNullValue(ValueType::tBOOLEAN);

    Polygon poly;
    poly.initFromGeography(arguments[0].getGeographyValue());
    S2Point pt = arguments[1].getGeographyPointValue().toS2Point();
    return ValueFactory::getBooleanValue(poly.Contains(pt));
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tINTEGER);
    }

    Polygon poly;
    poly.initFromGeography(getGeographyValue());

    NValue retVal(ValueType::tINTEGER);
    // exclude exterior ring
    retVal.getInteger() = poly.num_loops() - 1;
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_NUM_POINTS>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tINTEGER);
    }

    Polygon poly;
    poly.initFromGeography(getGeographyValue());

    // the OGC spec suggests that the number of vertices should
    // include the repeated closing vertex which is implicit in S2's
    // representation.  So add an extra vertex for each loop.
    int32_t numPoints = poly.num_vertices() + poly.num_loops();

    NValue retVal(ValueType::tINTEGER);
    retVal.getInteger() = numPoints;
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINT_LATITUDE>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tDOUBLE);
    }
    const GeographyPointValue point = getGeographyPointValue();
    NValue retVal(ValueType::tDOUBLE);
    retVal.getDouble() = point.getLatitude();
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINT_LONGITUDE>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tDOUBLE);
    }
    const GeographyPointValue point = getGeographyPointValue();
    NValue retVal(ValueType::tDOUBLE);
    retVal.getDouble() = point.getLongitude();
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_CENTROID>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tPOINT);
    }

    Polygon polygon;
    polygon.initFromGeography(getGeographyValue());
    const GeographyPointValue point(polygon.GetCentroid());
    NValue retVal(ValueType::tPOINT);
    retVal.getGeographyPointValue() = point;
    return retVal;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_AREA>() const {
    if (isNull()) {
        return NValue::getNullValue(ValueType::tDOUBLE);
    }

    Polygon polygon;
    polygon.initFromGeography(getGeographyValue());

    NValue retVal(ValueType::tDOUBLE);
    // area is in steradians which is a solid angle. Earth in the calculation is treated as sphere
    // and area of sphere can be calculated as steradians * radius^2
    retVal.getDouble() = polygon.GetArea() * RADIUS_SQ_M;
    return retVal;
}

template<> NValue NValue::call<FUNC_VOLT_DISTANCE_POLYGON_POINT>(const std::vector<NValue>& arguments) {
    vassert(arguments[0].getValueType() == ValueType::tGEOGRAPHY);
    vassert(arguments[1].getValueType() == ValueType::tPOINT);

    if (arguments[0].isNull() || arguments[1].isNull()) {
        return NValue::getNullValue(ValueType::tDOUBLE);
    }

    Polygon polygon;
    polygon.initFromGeography(arguments[0].getGeographyValue());
    GeographyPointValue point = arguments[1].getGeographyPointValue();
    NValue retVal(ValueType::tDOUBLE);
    // distance is in radians, so convert it to meters
    retVal.getDouble() = polygon.getDistance(point) * SPHERICAL_EARTH_MEAN_RADIUS_M;
    return retVal;
}

template<> NValue NValue::call<FUNC_VOLT_DISTANCE_POINT_POINT>(const std::vector<NValue>& arguments) {
    vassert(arguments[0].getValueType() == ValueType::tPOINT);
    vassert(arguments[1].getValueType() == ValueType::tPOINT);

    if (arguments[0].isNull() || arguments[1].isNull()) {
        return NValue::getNullValue(ValueType::tDOUBLE);
    } else {
        NValue retVal(ValueType::tDOUBLE);
        retVal.getDouble() = getDistance(arguments[0].getGeographyPointValue(), arguments[1].getGeographyPointValue());
        return retVal;
    }
}

template<> NValue NValue::callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT>() const {
    vassert(getValueType() == ValueType::tPOINT);
    if (isNull()) {
        return NValue::getNullValue(ValueType::tVARCHAR);
    } else {
        const std::string pointAsText = getGeographyPointValue().toWKT();
        return getTempStringValue(pointAsText.c_str(), pointAsText.length());
    }
}

template<> NValue NValue::callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY>() const {
    vassert(getValueType() == ValueType::tGEOGRAPHY);
    if (isNull()) {
        return NValue::getNullValue(ValueType::tVARCHAR);
    } else {
        const std::string polygonAsText = getGeographyValue().toWKT();
        return getTempStringValue(polygonAsText.c_str(), polygonAsText.length());
    }
}

//
// Return true if poly has more than one shell, or has shells
// inside holes.
//
static bool isMultiPolygon(const Polygon &poly, std::stringstream *msg) {
    auto nloops = poly.num_loops();
    int nouters = 0;
    for (int idx = 0; idx < nloops; idx += 1) {
        S2Loop *loop = poly.loop(idx);
        switch (loop->depth()) {
            case 0:
                nouters += 1;
                break;
            case 1:
                break;
            default:
                if (msg != NULL) {
                    (*msg) << "Polygons can only be shells or holes";
                } else {
                    VOLT_TRACE("Polygons can only be shells or holes.");
                }
                return true;
        }
        if (!loop->IsNormalized(msg)) {
            return true;
        }
    }
    if (nouters != 1) {
        if (msg != NULL) {
            (*msg) << "Polygons can have only one shell, not " << nouters;
        } else {
            VOLT_TRACE("Polygons can have only one shell, not %d.", nouters);
        }
        return true;
    } else {
        return false;
    }
}

/*
 * This and FUNC_VOLT_POLYGON_INVALID_REASON are suspiciously
 * close to the same thing.  Maybe they could be unified?
 */
template<> NValue NValue::callUnary<FUNC_VOLT_IS_VALID_POLYGON>() const {
    vassert(getValueType() == ValueType::tGEOGRAPHY);
    if (isNull()) {
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }
    // Be optimistic.
    bool returnval = true;
    // Extract the polygon and check its validity.
    Polygon poly;
    poly.initFromGeography(getGeographyValue());
    if (!poly.IsValid() || isMultiPolygon(poly, NULL)) {
        returnval = false;
    }
    return ValueFactory::getBooleanValue(returnval);
}

template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_INVALID_REASON>() const {
    vassert(getValueType() == ValueType::tGEOGRAPHY);
    if (isNull()) {
        return NValue::getNullValue(ValueType::tVARCHAR);
    }
    // Extract the polygon and check its validity.
    std::stringstream msg;
    Polygon poly;
    poly.initFromGeography(getGeographyValue());
    if (poly.IsValid(&msg)) {
        isMultiPolygon(poly, &msg);
    }
    std::string res (msg.str());
    if (res.size() == 0) {
        res = std::string("Valid Polygon");
    }
    return getTempStringValue(res.c_str(),res.length());
}

template<> NValue NValue::callUnary<FUNC_VOLT_MAKE_VALID_POLYGON>() const {
    vassert(getValueType() == ValueType::tGEOGRAPHY);
    if (isNull()) {
        return NValue::getNullValue(ValueType::tGEOGRAPHY);
    }
    // Extract the polygon and check its validity.
    std::stringstream msg;
    Polygon poly;
    poly.initFromGeography(getGeographyValue(), true);
    for (int idx = 0; idx < poly.num_loops(); idx += 1) {
        S2Loop *loop = poly.loop(idx);
        if ( ! loop->IsNormalized() ) {
            std::cout << "Not normalized loop in make_valid_polygon: " << idx << "\n";
        }
    }
    if ( ! poly.IsValid(&msg)) {
        std::string res (msg.str());
        vassert(res.size() > 0);
        throwInvalidMakeValidPoly(res);
        // No return from here.
    }
    else if ( isMultiPolygon(poly, &msg)) {
        std::string res (msg.str());
        vassert(res.size() > 0);
        throwInvalidMakeValidPoly(res);
        // No return from here.
    }
    // Ok, so the polygon either was valid before or else
    // we repaired it, and it is not a multi polygon.
    // So, msg will not be the empty string, and we can package
    // this polygon up.
    //
    vassert(msg.str().size() == 0);
    int length = poly.serializedLength();
    NValue nval = ValueFactory::getUninitializedTempGeographyValue(length);
    char* storage = const_cast<char*>(ValuePeeker::peekObjectValue(nval));
    SimpleOutputSerializer output(storage, length);
    poly.saveToBuffer(output);
    return nval;
}

template<> NValue NValue::call<FUNC_VOLT_DWITHIN_POLYGON_POINT>(const std::vector<NValue>& arguments) {
    vassert(arguments[0].getValueType() == ValueType::tGEOGRAPHY);
    vassert(arguments[1].getValueType() == ValueType::tPOINT);
    vassert(isNumeric(arguments[2].getValueType()));

    if (arguments[0].isNull() || arguments[1].isNull() || arguments[2].isNull()) {
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }

    Polygon polygon;
    polygon.initFromGeography(arguments[0].getGeographyValue());
    GeographyPointValue point = arguments[1].getGeographyPointValue();
    double withinDistanceOf = arguments[2].castAsDoubleAndGetValue();
    if (withinDistanceOf < 0) {
        throwInvalidDistanceDWithin("Value of DISTANCE argument must be non-negative");
    }
    double polygonToPointDistance = polygon.getDistance(point) * SPHERICAL_EARTH_MEAN_RADIUS_M;
    return ValueFactory::getBooleanValue(polygonToPointDistance <= withinDistanceOf);
}

template<> NValue NValue::call<FUNC_VOLT_DWITHIN_POINT_POINT>(const std::vector<NValue>& arguments) {
    vassert(arguments[0].getValueType() == ValueType::tPOINT);
    vassert(arguments[1].getValueType() == ValueType::tPOINT);
    vassert(isNumeric(arguments[2].getValueType()));

    if (arguments[0].isNull() || arguments[1].isNull() || arguments[2].isNull()) {
        return NValue::getNullValue(ValueType::tBOOLEAN);
    }

    double withinDistanceOf = arguments[2].castAsDoubleAndGetValue();
    if (withinDistanceOf < 0) {
        throwInvalidDistanceDWithin("Value of DISTANCE argument must be non-negative");
    }
    double pointToPointDistance = getDistance(arguments[0].getGeographyPointValue(), arguments[1].getGeographyPointValue());
    return ValueFactory::getBooleanValue(pointToPointDistance <= withinDistanceOf);
}
} // end namespace voltdb
