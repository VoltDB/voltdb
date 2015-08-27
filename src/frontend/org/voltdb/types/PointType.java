package org.voltdb.types;

public class PointType {

    private final boolean m_isNull;

    // Internal representation of a geospatial point
    // is subject to change.  For now, just use two doubles.
    private final double m_latitude;
    private final double m_longitude;

    public PointType() {
        m_isNull = true;
        m_latitude = 0.0f;
        m_longitude = 0.0f;
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

}
