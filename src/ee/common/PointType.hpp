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

#ifndef EE_COMMON_POINTTYPE_HPP
#define EE_COMMON_POINTTYPE_HPP

#include <limits>
#include <sstream>

namespace voltdb {
/**
 * A class for representing instances of geo-spatial points.
 * Stored as a pair of floats, so it fits into an 8-byte word.
 */
class PointType {
public:
    /** Constructor for a null point,
     * with both lat and lng init'd to NaN */
    PointType() {
        m_latitude = std::numeric_limits<float>::quiet_NaN();
        m_longitude = std::numeric_limits<float>::quiet_NaN();
    }

    PointType(float latitude, float longitude)
        : m_latitude(latitude)
        , m_longitude(longitude)
    {
    }

    /** Null values are represented as either lat or lng being NaN. */
    bool isNull() const {
        // NaN will not compare equal to itself.
        return (m_latitude != m_latitude) ||
            (m_longitude != m_longitude);
    }

    float getLatitude() const {
        return m_latitude;
    }

    float getLongitude() const {
        return m_longitude;
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "point(" << m_latitude << " " << m_longitude << ")";
        return oss.str();
    }

private:
    float m_latitude;
    float m_longitude;
};

} // end namespace

#endif // EE_COMMON_POINTTYPE_HPP
