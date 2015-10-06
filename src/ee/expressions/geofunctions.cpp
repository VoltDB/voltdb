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

static void throwInvalidWktPoly(const std::string& input)
{
    std::ostringstream oss;
    oss << "Invalid input to POLYGONFROMTEXT: ";
    oss << "'" << input << "', ";
    oss << "expected input of the form 'POLYGON((<lat> <lng>, ...), ...)'";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static Point::Coord stringToCoord(int pointOrPoly,
                                  const std::string& input,
                                  const std::string& val)
{
    Point::Coord coord = 0.0;
    try {
        coord = boost::lexical_cast<Point::Coord>(val);
    }
    catch (const boost::bad_lexical_cast&) {
        if (pointOrPoly == POLY) {
            throwInvalidWktPoly(input);
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


    Point::Coord lat = stringToCoord(POINT, wkt, *it);
    ++it;

    Point::Coord lng = stringToCoord(POINT, wkt, *it);
    ++it;

    if (! boost::iequals(*it, ")")) {
        throwInvalidWktPoint(wkt);
    }
    ++it;

    if (it != end) {
        throwInvalidWktPoint(wkt);
    }

    NValue returnValue(VALUE_TYPE_POINT);
    returnValue.getPoint() = Point(lat, lng);

    return returnValue;
}

static void readLoop(const std::string &wkt,
                     Tokenizer::iterator &it,
                     const Tokenizer::iterator &end,
                     std::vector<Point::Coord> *loop)
{
    if (! boost::iequals(*it, "(")) {
        throwInvalidWktPoly(wkt);
    }
    ++it;

    while (it != end && *it != ")") {
        Point::Coord lat = stringToCoord(POLY, wkt, *it);
        loop->push_back(lat);
        ++it;

        Point::Coord lng = stringToCoord(POLY, wkt, *it);
        loop->push_back(lng);
        ++it;

        if (*it == ",") {
            ++it;
            // continue to next lat long pair
        }
        else if (*it != ")") {
            throwInvalidWktPoly(wkt);
        }
    }

    if (it == end) {
        // we hit the end of input before the closing parenthesis
        throwInvalidWktPoly(wkt);
    }

    assert (*it == ")");

    // Advance iterator to next token
    ++it;
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
        throwInvalidWktPoly(wkt);
    }
    ++it;

    if (! boost::iequals(*it, "(")) {
        throwInvalidWktPoly(wkt);
    }
    ++it;

    std::vector<std::vector<Point::Coord> > loops;
    while (it != end) {
        std::vector<Point::Coord> loop;
        readLoop(wkt, it, end, &loop);
        // When we have C++11, use emplace_back here
        // to avoid a copy.
        loops.push_back(loop);
        if (*it == ",") {
            ++it;
        }
        else if (*it == ")") {
            ++it;
            break;
        }
        else {
            throwInvalidWktPoly(wkt);
        }
    }

    if (it != end) {
        // extra stuff after input
        throwInvalidWktPoly(wkt);
    }

    std::ostringstream oss;
    int32_t numLoops = static_cast<int32_t>(loops.size());
    oss.write(reinterpret_cast<char*>(&numLoops), sizeof (int32_t));
    BOOST_FOREACH(std::vector<Point::Coord>& loop, loops) {
        assert(loop.size() % 2 == 0);
        int32_t numVertices = static_cast<int32_t>(loop.size()) / 2;
        oss.write(reinterpret_cast<char*>(&numVertices), sizeof (int32_t));
        BOOST_FOREACH(Point::Coord coord, loop) {
            oss.write(reinterpret_cast<char*>(&coord), sizeof(coord));
        }
    }

    return ValueFactory::getTempGeographyValue(oss.str().c_str(),
                                               static_cast<int32_t>(oss.str().length()));
}

} // end namespace voltdb
