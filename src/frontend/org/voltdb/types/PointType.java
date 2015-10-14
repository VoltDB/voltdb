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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PointType {
	//
	// It's slightly hard to see this in the actual pattern
	// definition, but the pattern we want to match, ignoring space, is:
	//    point([1-9]*.[0-9]* , [1-9]*.[0-9]*)
	// Here the '.' is not 'match anything' but 'match a single dot'.  The
	// RE below does this right, though the simplified version above
	// does not.
	//
	private static final Pattern wktPattern 
		= Pattern.compile("^\\s*point\\s*[(]\\s*([-]?[1-9]\\d*)(?:[.](\\d*))?\\s+([-]?[1-9]\\d*)(?:[.](\\d*))?\\s*[)]\\s*\\z",
						  Pattern.CASE_INSENSITIVE);
    // Internal representation of a geospatial point
    // is subject to change.  For now, just use two floats.
    // This matches the EE representation.
    private final float m_latitude;
    private final float m_longitude;

    // In the default constructor, initialize to the null point
    // (defined as either value being NaN)
    public PointType() {
        m_latitude = Float.NaN;
        m_longitude = Float.NaN;
    }

    public PointType(float latitude, float longitude) {
        m_latitude = latitude;
        m_longitude = longitude;
    }

    private static float toFloat(String aInt, String aFrac) {
    	return Float.parseFloat(aInt + "." + (aFrac == null ? "0" : aFrac));
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
    		float latitude  = toFloat(m.group(1), m.group(2));
    		float longitude = toFloat(m.group(3), m.group(4));
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

	public boolean isNull() {
        return Float.isNaN(m_latitude) || Float.isNaN(m_longitude);
    }

    public float getLatitude() {
        return m_latitude;
    }

    public float getLongitude() {
        return m_longitude;
    }

    @Override
    public String toString() {
        if (isNull()) {
            return "NULL";
        }

        return "POINT (" + m_latitude + " " + m_longitude + ")";
    }

    // Returns true for two points that have the same latitude and
    // longitude.  For NULL points (defined as either lat or lng being NaN),
    // Return true if both points are null.
    //
    // In SQL two null values should not compare as equal, but in Java
    // (say for inserting unique values into a hash table), we really want
    // any two null points to be considered as equal.
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PointType)) {
            return false;
        }

        PointType that = (PointType)o;

        if (isNull() && that.isNull()) {
            return true;
        }

        if (that.getLatitude() != getLatitude()) {
            return false;
        }

        return that.getLongitude() == getLongitude();
    }

    /**
     * The number of bytes an instance of this class requires in serialized form.
     */
    static public int getLengthInBytes() {
        return 8;
    }

    /**
     * Serialize this instance to a byte buffer.
     * @param buffer
     */
    public void flattenToBuffer(ByteBuffer buffer) {
        buffer.putFloat(getLatitude());
        buffer.putFloat(getLongitude());
    }

    /**
     * Deserialize a point type from a byte buffer
     * @param inBuffer
     * @param offset    offset of point data in buffer
     * @return a new instance of PointType
     */
    public static PointType unflattenFromBuffer(ByteBuffer inBuffer, int offset) {
        float lat = inBuffer.getFloat(offset);
        float lng = inBuffer.getFloat(offset + 4);
        return new PointType(lat, lng);
    }

    /**
     * Deserialize a point type from a byte buffer
     * @param inBuffer
     * @param offset    offset of point data in buffer
     * @return a new instance of PointType
     */
    public static PointType unflattenFromBuffer(ByteBuffer inBuffer) {
        float lat = inBuffer.getFloat();
        float lng = inBuffer.getFloat();
        return new PointType(lat, lng);
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
