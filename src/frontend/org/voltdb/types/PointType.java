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
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PointType {
    //
    // It's slightly hard to see this in the actual pattern
    // definition, but the pattern we want to match, ignoring space, is:
    //    1. Some optional space.
    //    2. The word "point", case insensitive.
    //    3. Some optional space.
    //    4. A left parenthesis.
    //    5. Some optional space.
    //    6. A coordinate, consisting of
    //       6.1. An optional sign.
    //       6.2. An integer part, consisting of a non-empty sequence of digits.
    //       6.3. An optional fractional part, consisting of a
    //            dot followed by a non-empty sequence of digits.
    //    7. Some required space.
    //    8. A second coordinate, just like (6) above
    //    9. A right parenthesis.
    //   10. Some optional space.
    //   11. The end of the string.
    //
    private static final Pattern wktPattern
 = Pattern.compile("^\\s*point\\s*[(]\\s*([-]?\\d+)(?:[.](\\d*))?\\s+([-]?\\d+)(?:[.](\\d*))?\\s*[)]\\s*\\z",
                          Pattern.CASE_INSENSITIVE);
    private final double m_latitude;
    private final double m_longitude;

    private static final int BYTES_IN_A_COORD = Double.SIZE / 8;

    // We use this value to represent a null point.
    // (Only for sending data over the wire.  The client
    // should receive a null when retrieving null points from a
    // VoltTable.)
    static final double NULL_COORD = 360.0;

    public PointType(double latitude, double longitude) {
        m_latitude = latitude;
        m_longitude = longitude;

        if (m_latitude < -90.0 || m_latitude > 90.0) {
            throw new IllegalArgumentException("Latitude out of range in PointType constructor");
        }

        if (m_longitude < -180.0 || m_longitude > 180.0) {
            throw new IllegalArgumentException("Longitude out of range in PointType constructor");
        }
    }

    private static double toDouble(String aInt, String aFrac) {
        return Double.parseDouble(aInt + "." + (aFrac == null ? "0" : aFrac));
    }
    /**
     * Create a PointType from a WellKnownText string.
     * @param param
     */
    public static PointType pointFromText(String param) {
        if (param == null) {
            throw new IllegalArgumentException("Null well known text argument to PointType constructor.");
        }
        Matcher m = wktPattern.matcher(param);
        if (m.find()) {
            double latitude  = toDouble(m.group(1), m.group(2));
            double longitude = toDouble(m.group(3), m.group(4));
            if (Math.abs(latitude) > 90.0) {
                throw new IllegalArgumentException(String.format("Latitude \"%f\" out of bounds.", latitude));
            }
            if (Math.abs(longitude) > 180.0) {
                throw new IllegalArgumentException(String.format("Longitude \"%f\" out of bounds.", longitude));
            }
            return new PointType(latitude, longitude);
        } else {
            throw new IllegalArgumentException("Cannot construct PointType value from \"" + param + "\"");
        }
    }

    public double getLatitude() {
        return m_latitude;
    }

    public double getLongitude() {
        return m_longitude;
    }

    public String formatLatLng() {
        // Display a maximum of 9 decimal digits after the point.
        // This gives us precision of around 1 mm.
        DecimalFormat df = new DecimalFormat("##0.0########");
        return df.format(m_latitude) + " " + df.format(m_longitude);
    }

    @Override
    public String toString() {
        return "POINT (" + formatLatLng() + ")";
    }

    // Returns true for two points that have the same latitude and
    // longitude.
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

    /**
     * The number of bytes an instance of this class requires in serialized form.
     */
    static public int getLengthInBytes() {
        return BYTES_IN_A_COORD * 2;
    }

    /**
     * Serialize this instance to a byte buffer.
     * @param buffer
     */
    public void flattenToBuffer(ByteBuffer buffer) {
        buffer.putDouble(getLatitude());
        buffer.putDouble(getLongitude());
    }

    /**
     * Deserialize a point type from a byte buffer
     * @param inBuffer
     * @param offset    offset of point data in buffer
     * @return a new instance of PointType
     */
    public static PointType unflattenFromBuffer(ByteBuffer inBuffer, int offset) {
        double lat = inBuffer.getDouble(offset);
        double lng = inBuffer.getDouble(offset + BYTES_IN_A_COORD);
        if (lat == 360.0 && lng == 360.0) {
            // This is a null point.
            return null;
        }

        return new PointType(lat, lng);
    }

    /**
     * Deserialize a point type from a byte buffer
     * @param inBuffer
     * @param offset    offset of point data in buffer
     * @return a new instance of PointType
     */
    public static PointType unflattenFromBuffer(ByteBuffer inBuffer) {
        double lat = inBuffer.getDouble();
        double lng = inBuffer.getDouble();
        if (lat == 360.0 && lng == 360.0) {
            // This is a null point.
            return null;
        }

        return new PointType(lat, lng);
    }

    /**
     * Serialize the null point to a byte buffer
     * @param buffer
     */
    public static void serializeNull(ByteBuffer buffer) {
        buffer.putDouble(NULL_COORD);
        buffer.putDouble(NULL_COORD);
    }
}
