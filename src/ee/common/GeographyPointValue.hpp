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

#ifndef EE_COMMON_GEOGRAPHY_POINT_VALUE_HPP
#define EE_COMMON_GEOGRAPHY_POINT_VALUE_HPP

#include <limits>
#include <sstream>

#include "common/MiscUtil.h"
#include "common/value_defs.h"

#include "s2geo/s2.h"
#include "s2geo/s2latlng.h"

namespace voltdb {
/**
 * A class for representing instances of geo-spatial points.
 */
class GeographyPointValue {
public:

    typedef double Coord;

    /** Constructor for a null point,
     * with both lng and lat initialized to the null coordinate */
    GeographyPointValue()
        : m_latitude(nullCoord())
        , m_longitude(nullCoord())
    {
    }

    GeographyPointValue(Coord longitude, Coord latitude)
        : m_latitude(latitude)
        , m_longitude(longitude)
    {
        assert (m_latitude >= -90.0 && m_latitude <= 90.0);
        assert (m_longitude >= -180.0 && m_longitude <= 180.0);
    }

    GeographyPointValue(const S2Point &s2Point)
        : m_latitude(nullCoord())
        , m_longitude(nullCoord())
    {
        assert(!s2Point.IsNaN());
        S2LatLng latLong(s2Point);
        m_latitude = latLong.lat().degrees();
        m_longitude = latLong.lng().degrees();
        assert (m_latitude >= -90.0 && m_latitude <= 90.0);
        assert (m_longitude >= -180.0 && m_longitude <= 180.0);
    }

    // Use the number 360.0 for the null coordinate.
    static Coord nullCoord() {
        // A static const member could be used for this, but clang
        // wants the constexpr keyword to be used with floating-point
        // constants, and constexpr isn't supported until gcc 4.6.
        // Creating a static function seems nicer than suppressing the
        // warning or using the conditional compilation.
        return 360.0;
    }

    // The null point has 360 for both lat and long.
    bool isNull() const {
        return (m_latitude == nullCoord()) &&
            (m_longitude == nullCoord());
    }

    Coord getLatitude() const {
        return m_latitude;
    }

    Coord getLongitude() const {
        return m_longitude;
    }

    S2Point toS2Point() const {
        return S2LatLng::FromDegrees(getLatitude(), getLongitude()).ToPoint();
    }

    int compareWith(const GeographyPointValue& rhs) const {

        // Caller guarantees that neither side is null
        assert(! isNull());
        assert(! rhs.isNull());

        Coord lhsLong = getLongitude();
        Coord rhsLong = rhs.getLongitude();
        if (lhsLong < rhsLong) {
            return VALUE_COMPARE_LESSTHAN;
        }

        if (lhsLong > rhsLong) {
            return VALUE_COMPARE_GREATERTHAN;
        }

        // latitude is equal; compare longitude
        Coord lhsLat = getLatitude();
        Coord rhsLat = rhs.getLatitude();
        if (lhsLat < rhsLat) {
            return VALUE_COMPARE_LESSTHAN;
        }

        if (lhsLat > rhsLat) {
            return VALUE_COMPARE_GREATERTHAN;
        }

        return VALUE_COMPARE_EQUAL;
    }

    template<class Deserializer>
    static GeographyPointValue deserializeFrom(Deserializer& input) {
        Coord lng = input.readDouble();
        Coord lat = input.readDouble();
        if (lat == nullCoord() && lng == nullCoord()) {
            return GeographyPointValue();
        }

        return GeographyPointValue(lng, lat);
    }

    template<class Serializer>
    void serializeTo(Serializer& output) const {
        output.writeDouble(getLongitude());
        output.writeDouble(getLatitude());
    }

    void hashCombine(std::size_t& seed) const {
        MiscUtil::hashCombineFloatingPoint(seed, m_longitude);
        MiscUtil::hashCombineFloatingPoint(seed, m_latitude);
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "point(" << m_longitude << " " << m_latitude << ")";
        return oss.str();
    }

    std::string formatLngLat() const{
        std::ostringstream oss;
        oss << toString(m_longitude) << " " << toString(m_latitude);
        return oss.str();
    }

    std::string toWKT() const {
        std::ostringstream oss;
        oss <<"Point (" << formatLngLat() << ")";
        return oss.str();
    }

private:
    // converts double value to string with specified precision displaying
    // only significant decimal value.
    // Output pattern is similar to "...##0.0##..."
    std::string toString(double number, int decimalPrecision=12) const {
        double decimalNumber;
        char buffer[32];

        // provided precision should be less than that can survive roundtrip of double -> text -> double
#if __cplusplus > 199711L
        // if platform supports C++11
        assert(decimalPrecision <= std::numeric_limits< double >::max_digits10);
#else
        assert(decimalPrecision <= 17);
#endif

        decimalNumber = number - floor(number);
        if (decimalNumber == 0) {
            snprintf(buffer, sizeof(buffer), "%3.1f", number);
        }
        else {
            int wholeNumberDigits = log10(abs(number)) + 1;
            snprintf(buffer, sizeof(buffer), "%.*g", (wholeNumberDigits + decimalPrecision), number);
        }
        return buffer;
    }

private:
    Coord m_latitude;
    Coord m_longitude;
};

} // end namespace

#endif // EE_COMMON_GEOGRAPHY_POINT_VALUE_HPP
