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
#include <boost/lexical_cast.hpp>

#include "expressions/geofunctions.h"
#include "s2geo/s2latlng.h"

namespace voltdb {

/**
 * This function is here only to verify that we can
 * link with the S2 Geometry Library.
 */
void verifyS2Links() {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
    S2LatLng  Bedford{S2LatLng::FromDegrees(42.4906, -71.2767)};
    S2LatLng  SantCruz{S2LatLng::FromDegrees(39.9719, -122.0264)};
    // The GetDistance function is out-of-line.  So, we can
    // see that it's linked in with nm.
    S1Angle d = Bedford.GetDistance(SantCruz);
#pragma clang diagnostic pop

}

static void throwInvalidWKT(const std::string& input)
{
    std::ostringstream oss;
    oss << "Invalid input to POINTFROMTEXT: ";
    oss << "'" << input << "', ";
    oss << "expected input of the form 'POINT(<lat> <lng>)'";
    throw SQLException(SQLException::data_exception_invalid_parameter,
                       oss.str().c_str());
}

static float stringToFloat(const std::string& input,
                           const std::string& val)
{
    float f = 0.0;
    try {
        f = boost::lexical_cast<float>(val);
    }
    catch (const boost::bad_lexical_cast&) {
        throwInvalidWKT(input);
    }

    return f;
}

template<> NValue NValue::callUnary<FUNC_VOLT_POINTFROMTEXT>() const
{
    if (isNull()) {
        return NValue::getNullValue(VALUE_TYPE_POINT);
    }

    std::string origInput(reinterpret_cast<char*>(getObjectValue_withoutNull()),
                          getObjectLength_withoutNull());
    std::string arg = origInput;

    boost::trim(arg);

    if (!boost::istarts_with(arg, "point")) {
        throwInvalidWKT(origInput);
    }

    boost::erase_head(arg, 5);
    boost::trim(arg);

    if (!boost::istarts_with(arg, "(")
        || !boost::iends_with(arg, ")")) {
        throwInvalidWKT(origInput);
    }

    boost::erase_head(arg, 1);
    boost::erase_tail(arg, 1);
    boost::trim(arg);

    std::vector<std::string> latLng(2);
    boost::split(latLng, arg, boost::is_space(), boost::token_compress_on);

    if (latLng.size() != 2) {
        throwInvalidWKT(origInput);
    }

    float lat = stringToFloat(origInput, latLng[0]);
    float lng = stringToFloat(origInput, latLng[1]);

    NValue returnValue(VALUE_TYPE_POINT);
    returnValue.getPoint() = Point(lat, lng);

    return returnValue;
}

} // end namespace voltdb
