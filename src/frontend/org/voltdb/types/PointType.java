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

package org.voltdb.types;

public class PointType {

    private final boolean m_isNull;

    // Internal representation of a geospatial point
    // is subject to change.  For now, just use two doubles.
    private final double m_latitude;
    private final double m_longitude;

    public PointType() {
        m_isNull = true;
        m_latitude = Float.MIN_VALUE;
        m_longitude = Float.MIN_VALUE;
    }

    public boolean isNull() {
        return m_isNull;
    }

    public double getLatitude() {
        return m_latitude;
    }

    public double getLongitude() {
        return m_longitude;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PointType)) {
            return false;
        }

        PointType that = (PointType)o;
        if (that.getLatitude() != getLatitude()) {
            return false;
        }

        return that.getLongitude() == getLongitude();
    }
}
