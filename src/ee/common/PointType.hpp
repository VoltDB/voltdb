/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

#include <limits>
#include <sstream>

#ifndef EE_COMMON_POINTTYPE_HPP
#define EE_COMMON_POINTTYPE_HPP

namespace voltdb {
class PointType {
public:
    PointType() {
        m_latitude = std::numeric_limits<float>::quiet_NaN();
        m_longitude = std::numeric_limits<float>::quiet_NaN();
    }

    PointType(float latitude, float longitude)
        : m_latitude(latitude)
        , m_longitude(longitude)
    {
    }

    bool isNull() const {
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
