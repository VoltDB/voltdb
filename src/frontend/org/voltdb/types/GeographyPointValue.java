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

public class GeographyPointValue {
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

    public GeographyPointValue(double longitude, double latitude) {
        // Add 0.0 to avoid -0.0.
        m_latitude = latitude + 0.0;
        m_longitude = longitude + 0.0;

        if (m_latitude < -90.0 || m_latitude > 90.0) {
            throw new IllegalArgumentException("Latitude out of range in GeographyPointValue constructor");
        }

        if (m_longitude < -180.0 || m_longitude > 180.0) {
            throw new IllegalArgumentException("Longitude out of range in GeographyPointValue constructor");
        }
    }

    private static double toDouble(String aInt, String aFrac) {
        return Double.parseDouble(aInt + "." + (aFrac == null ? "0" : aFrac));
    }
    /**
     * Create a GeographyPointValue from a WellKnownText string.
     * @param param
     */
    public static GeographyPointValue geographyPointFromText(String param) {
        if (param == null) {
            throw new IllegalArgumentException("Null well known text argument to GeographyPointValue constructor.");
        }
        Matcher m = wktPattern.matcher(param);
        if (m.find()) {
            // Add 0.0 to avoid -0.0.
            double longitude = toDouble(m.group(1), m.group(2)) + 0.0;
            double latitude  = toDouble(m.group(3), m.group(4)) + 0.0;
            if (Math.abs(latitude) > 90.0) {
                throw new IllegalArgumentException(String.format("Latitude \"%f\" out of bounds.", latitude));
            }
            if (Math.abs(longitude) > 180.0) {
                throw new IllegalArgumentException(String.format("Longitude \"%f\" out of bounds.", longitude));
            }
            return new GeographyPointValue(longitude, latitude);
        } else {
            throw new IllegalArgumentException("Cannot construct GeographyPointValue value from \"" + param + "\"");
        }
    }

    public double getLatitude() {
        return m_latitude;
    }

    public double getLongitude() {
        return m_longitude;
    }


    /**
     * Format this point as WKT.  Use 12 digits of precision.
     */
    public String formatLngLat() {
        DecimalFormat df = new DecimalFormat("##0.0###########");

        // Explicitly test for differences less than 1.0e-12 and
        // force them to be zero.  Otherwise you may find a case
        // where two points differ in the less significant bits, but
        // they format as the same number.
        double lng = (Math.abs(m_longitude) < 1.0e-12) ? 0 : m_longitude;
        double lat = (Math.abs(m_latitude) < 1.0e-12) ? 0 : m_latitude;
        return df.format(lng) + " " + df.format(lat);
    }

    @Override
    public String toString() {
        // This is not GEOGRAPHY_POINT.  This is wkt syntax.
        return "POINT (" + formatLngLat() + ")";
    }

    // Returns true for two points that have the same latitude and
    // longitude.
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GeographyPointValue)) {
            return false;
        }

        GeographyPointValue that = (GeographyPointValue)o;

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
        buffer.putDouble(getLongitude());
        buffer.putDouble(getLatitude());
    }

    /**
     * Deserialize a point type from a byte buffer
     * @param inBuffer
     * @param offset    offset of point data in buffer
     * @return a new instance of GeographyPointValue
     */
    public static GeographyPointValue unflattenFromBuffer(ByteBuffer inBuffer, int offset) {
        double lng = inBuffer.getDouble(offset);
        double lat = inBuffer.getDouble(offset + BYTES_IN_A_COORD);
        if (lat == 360.0 && lng == 360.0) {
            // This is a null point.
            return null;
        }

        return new GeographyPointValue(lng, lat);
    }

    /**
     * Deserialize a point type from a byte buffer
     * @param inBuffer
     * @param offset    offset of point data in buffer
     * @return a new instance of GeographyPointValue
     */
    public static GeographyPointValue unflattenFromBuffer(ByteBuffer inBuffer) {
        double lng = inBuffer.getDouble();
        double lat = inBuffer.getDouble();
        if (lat == 360.0 && lng == 360.0) {
            // This is a null point.
            return null;
        }

        return new GeographyPointValue(lng, lat);
    }

    /**
     * Serialize the null point to a byte buffer
     * @param buffer
     */
    public static void serializeNull(ByteBuffer buffer) {
        buffer.putDouble(NULL_COORD);
        buffer.putDouble(NULL_COORD);
    }

    /**
     * Create a GeographyPointValue with normal coordinated.  The longitude and
     * latitude inputs may be any real numbers.  They are not restricted to be in
     * the ranges [-180,180] or [-90,90] respectively.  We will calculate the equivalent
     * longitude and latitude which is in these ranges and create a new GeographyPointValue.
     *
     * @param longitude
     * @param latitude
     * @return A GeographyPointValue with the given coordinates normalized.
     */
    public static GeographyPointValue normalizeLngLat(double longitude, double latitude) {
        // Now compute the latitude.  We compute this in
        // the range [-180, 180] and then fiddle with it.
        // If it's out of the range [-90, 90], we need to
        // flip it and then change the longitude.
        double latNorm = normalize(latitude, 360);
        double lngNorm = normalize(longitude, 360);
        double latFinal = 0.0;
        double lngFinal = 0.0;
        assert(-180 <= latNorm && latNorm <= 180);
        assert(-180 <= lngNorm && lngNorm <= 180);
        // Now, latOrig is the latitude in the range [-180,180].
        // Let latWant be latitude in the range [-90, 90]. Then
        // o If latOrig > 90, we have latOrig + latWant = 180.  That
        //   is, if we rotate our point starting at latOrig through
        //   the angle latWant, we get +180.</li>
        // o If latOrig < 90, then we have latOrig - latWant = -180.
        //   If we rotate our point, which is the southern hemisphere,
        //   through the angle latWant, then we will have it at -180.</li>
        //
        // In either case, if we change the latitude we then need to change
        // the longitude.  We want to reflect the longitude across the origin.
        boolean flipLng = false;
        if (latNorm > 90) {
            // This is latWant.
            latFinal = 180 - latNorm;
            flipLng = true;
        } else if (latNorm < -90) {
            // This is lngWant.
            latFinal = -latNorm - 180;
            flipLng = true;
        } else {
            latFinal = latNorm;
            lngFinal = lngNorm;
        }
        if (flipLng) {
            if (lngNorm <= 0) {
                // Since lngOrig is in [-180,0], we must have that
                // lngOrig + 180 is in the range [0, 180].
                // So, we are mapping 0 to 180.
                lngFinal = lngNorm + 180;
            } else {
                // Since lngOrig is in the range [-0, 180] we must have
                // that lngOrig - 180 is in the range [-180, 0].
                // So, we are mapping -0 to -180.
                lngFinal = lngNorm - 180;
            }
        }
        assert(-180 <= lngFinal && lngFinal <= 180);
        assert(-90 <= latFinal && latFinal <= 90);
        // Return the point.
        return new GeographyPointValue(lngFinal + 0.0, latFinal + 0.0);
    }

    // Normalize the value v to be in range [-range, range]
    // by subtracting multiples of 360.
    private static double normalize(double v, double range) {
        double a = v-Math.floor((v + (range/2))/range)*range;
        // Make sure that a and v have the same sign
        // when abs(v) = 180.
        if (Math.abs(a) == 180.0 && (a * v) < 0) {
            a *= -1;
        }
        // The addition of 0.0 is to avoid negative
        // zero, which just confuses things.
        return a + 0.0;
    }

    /**
     * Return a point which is offset by the given offset point.  The
     * offset point is scaled by alpha.  That is, the return value is
     * <code> this + alpha*offset</code>, except that Java does not
     * allow us to write it in this way.
     *
     * In particular, <code>this - other</code> is equal to
     * <code> add(other, -1)</code>, but fewer temporary objects are
     * created.
     *
     * Normalize the coordinates.
     * @param offset
     * @return A new point offset by the scaled offset.
     */
    public GeographyPointValue add(GeographyPointValue offset, double alpha) {
        // The addition of 0.0 converts
        // -0.0 to 0.0.
        return GeographyPointValue.normalizeLngLat(getLongitude() + alpha * offset.getLongitude() + 0.0,
                                                   getLatitude()  + alpha * offset.getLatitude() + 0.0);
    }

    /**
     * Add two points, and return a new point.
     *
     * @param offset The offset to add in.
     * @return A new point which is this plus the offset.
     */
    public GeographyPointValue add(GeographyPointValue offset) {
        return add(offset, 1.0);
    }

    /**
     * Return <code>this - offset</code>.
     *
     * @param offset The offset to subtract from this.
     * @return A new point translated by -offset.
     */
    public GeographyPointValue sub(GeographyPointValue offset) {
        return add(offset, -1.0);
    }

    public GeographyPointValue sub(GeographyPointValue offset, double scale) {
        return add(offset, -1.0 * scale);
    }

    /**
     * Return a point scaled by the given alpha value.
     *
     * @param alpha
     * @return The scaled point.
     */
    public GeographyPointValue mul(double alpha) {
        return GeographyPointValue.normalizeLngLat(getLongitude() * alpha + 0.0,
                                                   getLatitude() * alpha  + 0.0);
    }

    /**
     * Return a new point which is this point rotated by the angle phi around a given center point.
     *
     * @param phi The angle to rotate.
     * @param center The center of rotation.
     * @return A new, rotated point.
     */
    public GeographyPointValue rotate(double phi, GeographyPointValue center) {
        double sinphi = Math.sin(2*Math.PI*phi/360.0);
        double cosphi = Math.cos(2*Math.PI*phi/360.0);

        // Translate to the center.
        double longitude = getLongitude() - center.getLongitude();
        double latitude = getLatitude() - center.getLatitude();
        // Rotate and translate back.
        return GeographyPointValue.normalizeLngLat((cosphi * longitude - sinphi * latitude) + center.getLongitude(),
                                                   (sinphi * longitude + cosphi * latitude) + center.getLatitude());
    }

    /**
     * Return <code>alpha*(this - center) + center</code>.  This is
     * used to scale the vector from center to this as an offset by
     * alpha.  This is equivalent to <code>this.sub(center).mul(alpha).add(center)</code>,
     * but with fewer object creations.
     *
     * @param center The origin of scaling.
     * @param alpha The scale factor.
     * @return
     */
    public GeographyPointValue scale(GeographyPointValue center, double alpha) {
        return GeographyPointValue.normalizeLngLat(alpha*(getLongitude()-center.getLongitude()) + center.getLongitude(),
                                                   alpha*(getLatitude()-center.getLatitude()) + center.getLatitude());
    }
}
