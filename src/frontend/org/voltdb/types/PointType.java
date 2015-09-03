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

import java.nio.ByteBuffer;

public class PointType {

    // Internal representation of a geospatial point
    // is subject to change.  For now, just use two floats.
    private final float m_latitude;
    private final float m_longitude;

    public PointType() {
        m_latitude = Float.NaN;
        m_longitude = Float.NaN;
    }

    public PointType(float latitude, float longitude) {
        m_latitude = latitude;
        m_longitude = longitude;
    }

    public boolean isNull() {
        return Float.isNaN(m_latitude) || Float.isNaN(m_longitude);
    }

    public float getLatitude() {
        return m_latitude;
    }

    public float getLongitude() {
        return m_longitude;
    }

    public String toString() {
        if (isNull()) {
            return "NULL";
        }

        return "POINT (" + m_latitude + " " + m_longitude + ")";
    }

    // Always returns false for null points (either lat or long NaN)
    // What's the right thing to do here?
    //    In SQL semantics, null values should not compare equal to eachother.
    //    In Java, an instance should be equal to itself.
    //    For inserting into a hash table in Java, we'd want nulls to compare as equal.
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PointType)) {
            return false;
        }

        PointType that = (PointType)o;

        // Points that are both null are considered equal,
        // which breaks SQL semantics, but seems to make more
        // sense in Java.
        if (isNull() && that.isNull()) {
            return true;
        }

        if (that.getLatitude() != getLatitude()) {
            return false;
        }

        return that.getLongitude() == getLongitude();
    }

    /**
     * Serialize this instance to a byte buffer.
     * For now we serialize as a single-precision float (subject to change)
     * @param buffer
     */
    public void flattenToBuffer(ByteBuffer buffer) {
        buffer.putFloat((float)getLatitude());
        buffer.putFloat((float)getLongitude());
    }


    /**
     * Serialize the null point to a byte buffer
     * @param buffer
     */
    public static void serializeNull(ByteBuffer buffer) {
        buffer.putFloat(Float.NaN);
        buffer.putFloat(Float.NaN);
    }
}
