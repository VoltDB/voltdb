/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.DataOutput;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The Java class used to represent data with the SQL type GEOGRAPHY.
 * For now, this means polygons, but may include other kinds of geospatial
 * types in the future.
 */
public class GeographyValue {

    // Note that Google S2 refers to each ring of a polygon as a "loop"
    // whereas VoltDB uses the term "ring" or "linear ring" in documentation
    // and comments on public APIs.  In other places in the code, the terms
    // "loop" and "ring" are used interchangeably.

    /**
     * The default length (in bytes) for a column with type GEOGRAPHY, if no
     * length is specified.
     */
    public static final int DEFAULT_LENGTH = 32768;

    /**
     * The minimum-allowed length (in bytes) for a column with type GEOGRAPHY.
     * This is the length of a polygon with just three vertices.
     */
    public static final int MIN_SERIALIZED_LENGTH = 155; // number of bytes needed to store a triangle

    /**
     * The maximum-allowed length (in bytes) for a column with type GEOGRAPHY.
     * This is the usual max column length.
     */
    public static final int MAX_SERIALIZED_LENGTH = 1048576;

    //
    // This is a list of loops.  Each loop must be in
    // S2Loop format.  That is to say, it must have type XYZPoint
    // type, it must be in counter-clockwise order and it must not
    // be closed.  All loops, even holes, are CCW.
    //
    private List<List<XYZPoint>> m_loops;

     /**
     * Create a polygon from a list of rings.  Each ring is a list of points:
     * <ol>
     *   <li>The first ring in the list is the outer ring, also known as the
     *       shell.</li>
     *   <li>Subsequent rings should be inside of the outer ring and represent
     *       "holes" in the polygon.</li>
     *   <li>The shell should have its vertices listed in counter-clockwise order,
     *       so that the area inside the ring is on the left side of the line segments
     *       formed by adjacent vertices.</li>
     *   <li>Each hole, or inner ring, should have its vertices listed in clockwise
     *       order, so that the area inside the ring (the "hole") is on the right side
     *       of the line segments formed by adjacent vertices.</li>
     * </ol>
     * Note that this is the same as the order expected by the OGC standard's
     * Well-Known Text format.
     *
     * Note also that the rings here are lists of GeographyPointValues, and that they
     * are closed.  That is to say, the first vertex and the last
     * vertex must be equal.
     *
     * @param rings  A list of lists of points that will form a polygon.
     */
     public GeographyValue(List<List<GeographyPointValue>> rings) {
         this(rings, false);
     }

     /**
      * Create a polygon from a list of rings.  This is internal.  If the
      * argument ringsAreInS2Order is true then we will not reorder any holes.
      * Since the OGC text format, WKT, used CCW shells and CW holes, and S2
      * uses all CCW loops, we need to reverse holes for S2, and this is the
      * vertex order we use internally.  But if this is an operation on rings
      * which are already in S2 order, say for the polynomial creation or
      * algebra operations, we don't want to reorder the loops again.  The
      * argument holesAreInS2Order is true in this case.
      *
      * @param rings
      * @param holesAreInS2Order
      */
     private GeographyValue(List<List<GeographyPointValue>> rings,
                            boolean holesAreInS2Order) {
         if (rings == null || rings.size() < 1) {
             throw new IllegalArgumentException("GeographyValue must be instantiated with at least one ring");
         }
         // Note that we need to reverse all but the
         // first loop, since the EE wants them all in CCW order,
         // and the OGC order for holes is CW.
         //
         m_loops = new ArrayList<>();
         boolean firstLoop = true;
         for (List<GeographyPointValue> loop : rings) {
             diagnoseLoop(loop, "Invalid loop for GeographyValue: ");
             List<XYZPoint> oneLoop = new ArrayList<>();
             int startIdx;
             int endIdx;
             int delta;
             if (firstLoop || holesAreInS2Order) {
                 startIdx = 1;
                 // Don't copy the last vertex.
                 endIdx = loop.size() - 1;
                 delta = 1;
             } else {
                 // Don't copy the last vertex.
                 startIdx = loop.size() - 2;
                 endIdx = 0;
                 delta = -1;
             }
             oneLoop.add(XYZPoint.fromGeographyPointValue(loop.get(0)));
             for (int i = startIdx; i != endIdx; i += delta) {
                 oneLoop.add(XYZPoint.fromGeographyPointValue(loop.get(i)));
             }
             m_loops.add(oneLoop);
             firstLoop = false;
         }
    }

    /**
     * Create a GeographyValue object from an OGC well-known text-formatted string.
     * Currently only polygons can be created via this method.
     *
     * Well-known text format for polygons is composed of the "POLYGON" keyword
     * followed by a list of rings enclosed in parenthesis.  For example:
     * <p><tt>
     *   POLYGON((0 0, 20 0, 20 20, 0 20, 0 0),(5 5, 5 15, 15 15, 15 5, 5 5))
     * </tt></p>
     * Each point in a ring is composed of a coordinate of longitude and a coordinate
     * of latitude separated by a space. Note that longitude comes first in this notation.
     *
     * Additional notes about rings:
     * <ol>
     *   <li>The first ring in the list is the outer ring, also known as the
     *       shell.</li>
     *   <li>Subsequent rings should be inside of the outer ring and represent
     *       "holes" in the polygon.</li>
     *   <li>The shell should have its vertices listed in counter-clockwise order,
     *       so that the area inside the ring is on the left side of the line segments
     *       formed by adjacent vertices.</li>
     *   <li>Each hole, or inner ring, should have its vertices listed in clockwise
     *       order, so that the area inside the ring (the "hole") is on the right side
     *       of the line segments formed by adjacent vertices.</li>
     *   <li>Each ring must be closed; that is, the last point in the ring must be
     *       equal to this first.</li>
     * </ol>
     *
     * @param wkt  A well-known text-formatted string for a polygon.
     */
    public GeographyValue(String wkt) {
        if (wkt == null) {
            throw new IllegalArgumentException("Argument to GeographyValue WKT constructor was null");
        }

        m_loops = loopsFromWkt(wkt);

        if (m_loops == null || m_loops.isEmpty()) {
            throw new IllegalArgumentException("Argument to GeographyValue WKT constructor was invalid");
        }
    }

    /**
     * Create a GeographyValue object from a well-known text string.
     * This format is described in {@link #GeographyValue(String) the WKT constructor}
     * for this class.
     *
     * @param text A well-known text string
     * @return A new instance of GeographyValue
     */
    public static GeographyValue fromWKT(String text) {
        return new GeographyValue(text);
    }

    /**
     * Return the list of rings of a polygon.  The list has the same
     * values as the list of rings used to construct the polygon, or
     * the sequence of WKT rings used to construct the polygon.
     *
     * @return A list of rings.
     */
    public List<List<GeographyPointValue>> getRings() {
        /*
         * Gets the loops that make up the polygon, with the outer loop first.
         * Note that we need to convert from XYZPoint to GeographyPointValue.
         *
         * Include the loop back to the first vertex.  Also, since WKT wants
         * holes oriented Clockwise and S2 wants everything oriented CounterClockWise,
         * reverse the order of holes.  We take care to leave the first vertex
         * the same.
         */
        List<List<GeographyPointValue>> llLoops = new ArrayList<>();

        boolean isShell = true;
        for (List<XYZPoint> xyzLoop : m_loops) {
            List<GeographyPointValue> llLoop = new ArrayList<>();
            // Add the first of xyzLoop first.
            llLoop.add(xyzLoop.get(0).toGeographyPointValue());
            // Add shells left to right, and holes right to left.  Make sure
            // not to add the first element we just added.
            int startIdx = (isShell ? 1              : xyzLoop.size()-1);
            int endIdx   = (isShell ? xyzLoop.size() : 0);
            int delta    = (isShell ? 1              : -1);
            for (int idx = startIdx; idx != endIdx; idx += delta) {
                XYZPoint xyz = xyzLoop.get(idx);
                llLoop.add(xyz.toGeographyPointValue());
            }
            // Close the loop.
            llLoop.add(xyzLoop.get(0).toGeographyPointValue());
            llLoops.add(llLoop);
            isShell = false;
        }
        return llLoops;
    }

    /**
     * Return a representation of this object as well-known text.
     * @return A well-known text string for this object.
     */
    @Override
    public String toString() {
        return toWKT();
    }

    /**
     * Return a representation of this object as well-known text.
     * @return A well-known text string for this object.
     */
    public String toWKT() {
        StringBuffer sb = new StringBuffer();
        sb.append("POLYGON (");

        boolean isFirstLoop = true;
        for (List<XYZPoint> loop : m_loops) {
            if (!isFirstLoop) {
                sb.append(", ");
            }

            sb.append("(");
            int startIdx = (isFirstLoop ? 1 : loop.size()-1);
            int endIdx = (isFirstLoop ? loop.size() : 0);
            int increment = (isFirstLoop ? 1 : -1);
            sb.append(loop.get(0).toGeographyPointValue().formatLngLat()).append(", ");
            for (int idx = startIdx; idx != endIdx; idx += increment) {
                XYZPoint xyz = loop.get(idx);
                sb.append(xyz.toGeographyPointValue().formatLngLat());
                sb.append(", ");
            }

            // Repeat the start vertex to close the loop as WKT requires.
            sb.append(loop.get(0).toGeographyPointValue().formatLngLat());
            sb.append(")");
            isFirstLoop = false;
        }

        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GeographyValue)) {
            return false;
        }

        GeographyValue that = (GeographyValue)o;
        if (this == that) {
            return true;
        }

        List<List<GeographyPointValue>> expectedRings = that.getRings();
        List<List<GeographyPointValue>> actualRings = getRings();

        // check number of rings/loops
        if (expectedRings.size() != actualRings.size()) {
            return false;
        }

        Iterator<List<GeographyPointValue>> expectedRingIt = expectedRings.iterator();
        for (List<GeographyPointValue> actualRing : actualRings) {
            List<GeographyPointValue> expectedRing = expectedRingIt.next();

            // check if number of the vertices in loops are equal
            if (expectedRing.size() != actualRing.size()) {
                return false;
            };

            Iterator<GeographyPointValue> expectedVertexIt = expectedRing.iterator();
            for (GeographyPointValue actualPt : actualRing) {
                GeographyPointValue expectedPt = expectedVertexIt.next();
                if (!expectedPt.equals(actualPt)) {
                    return false;
                }
            }
        }

        return true;
    }

    /* Serialization format for polygons.
     *
     * This is the format used by S2 in the EE.  Most of the
     * metadata (especially lat/lng rect bounding boxes) are
     * ignored here in Java.
     *
     * 1 byte       encoding version
     * 1 byte       boolean owns_loops
     * 1 byte       boolean has_holes
     * 4 bytes      number of loops
     *   And then for each loop:
     *     1 byte       encoding version
     *     4 bytes      number of vertices
     *     ((number of vertices) * sizeof(double) * 3) bytes    vertices as XYZPoints
     *     1 byte       boolean origin_inside
     *     4 bytes      depth (nesting level of loop)
     *     33 bytes     bounding box
     * 33 bytes     bounding box
     *
     * We use S2 in the EE for all geometric computation, so polygons sent to
     * the EE will be missing bounding box and other info.  We indicate this
     * by passing INCOMPLETE_ENCODING_FROM_JAVA in the version field.  This
     * tells the EE to compute bounding boxes and other metadata before storing
     * the polygon to memory.
     */

    private static final byte INCOMPLETE_ENCODING_FROM_JAVA = 0;
    private static final byte COMPLETE_ENCODING = 1;

    private static long polygonOverheadInBytes() {
        return 7 + boundLengthInBytes();
    }

    /**
     * Return the number of bytes in the serialization for this polygon.
     * Returned value does not include the 4-byte length prefix that precedes variable-length types.
     * @return The number of bytes in the serialization for this polygon.
     */
    public int getLengthInBytes() {
        long length = polygonOverheadInBytes();
        for (List<XYZPoint> loop : m_loops) {
            length += loopLengthInBytes(loop.size());
        }

        return (int)length;
    }

    /**
     * Given a column of type GEOGRAPHY(nbytes), return an upper bound on the
     * number of characters needed to represent any entity of this type in WKT.
     * @param numBytes  The size of the GEOGRAPHY value in bytes
     * @return Upper bound of characters needed for WKT string
     */
    public static int getValueDisplaySize(int numBytes) {
        if (numBytes < MIN_SERIALIZED_LENGTH) {
            throw new IllegalArgumentException("Cannot compute max display size for a GEOGRAPHY value of size "
                    + numBytes + " bytes, since minimum allowed size is " + MIN_SERIALIZED_LENGTH);
        }

        // Vertices will dominate the WKT output, so compute the maximum
        // number of vertices given the number of bytes.  This will be a polygon
        // with just one loop.
        int numBytesUsedForVertices = numBytes;
        numBytesUsedForVertices -= polygonOverheadInBytes();
        numBytesUsedForVertices -= loopOverheadInBytes();

        int numVertices = numBytesUsedForVertices / 24;

        // display size will be
        // "POLYGON (())" [12 bytes]
        // plus the number of bytes used by vertices:
        // "-180.123456789012 -90.123456789012, " [max of 36 bytes per vertex]
        return 12 + 36 * numVertices;
    }

    /**
     * Serialize this object to a ByteBuffer.
     * (Assumes that the 4-byte length prefix for variable-length data
     * has already been serialized.)
     *
     * @param buf  The ByteBuffer into which the serialization will be placed.
     */
    public void flattenToBuffer(ByteBuffer buf) {
        buf.put(INCOMPLETE_ENCODING_FROM_JAVA); // encoding version
        buf.put((byte)1); // owns_loops_
        buf.put((byte)(m_loops.size() > 1 ? 1 : 0)); // has_holes_
        buf.putInt(m_loops.size());
        int depth = 0;
        for (List<XYZPoint> loop : m_loops) {
            flattenLoopToBuffer(loop, depth, buf);
            depth = 1;
        }
        flattenEmptyBoundToBuffer(buf);
    }

    /**
     * Serialize this object to a {@link DataOutput}
     * <p>
     * To be consisitent with {@link #flattenToBuffer(ByteBuffer)} <br>
     * (Assumes that the 4-byte length prefix for variable-length data has already been serialized.)
     *
     * @param output into which this object will be serialized
     * @throws IOException if any I/O error occurs
     */
    public void serialize(DataOutput output) throws IOException {
        output.write(INCOMPLETE_ENCODING_FROM_JAVA);
        output.write(1);
        output.write(m_loops.isEmpty() ? 0 : 1);
        output.writeInt(m_loops.size());
        int depth = 0;
        for (List<XYZPoint> loop : m_loops) {
            serializeLoop(loop, depth, output);
            depth = 1;
        }
        serializeEmptyBound(output);
    }

    /**
     * Deserialize a GeographyValue from a ByteBuffer from an absolute offset.
     * (Assumes that the 4-byte length prefix has already been deserialized, and that
     * offset points to the start of data just after the prefix.)
     * @param inBuffer  The ByteBuffer from which to read a GeographyValue
     * @param offset    The absolute offset in the ByteBuffer from which to read data
     * @return A new GeographyValue instance.
     */
    public static GeographyValue unflattenFromBuffer(ByteBuffer inBuffer, int offset) {
        int origPos = inBuffer.position();
        inBuffer.position(offset);
        GeographyValue gv = unflattenFromBuffer(inBuffer);
        inBuffer.position(origPos);
        return gv;
    }

    /**
     * Deserialize a GeographyValue from a ByteBuffer at the ByteBuffer's
     * current position.
     * (Assumes that the 4-byte length prefix has already been deserialized.)
     * @param inBuffer  The ByteBuffer from which to read a GeographyValue
     * @return A new GeographyValue instance.
     */
    public static GeographyValue unflattenFromBuffer(ByteBuffer inBuffer) {
        byte version = inBuffer.get(); // encoding version
        inBuffer.get(); // owns loops
        inBuffer.get(); // has holes
        int numLoops = inBuffer.getInt();
        List<List<XYZPoint>> loops = new ArrayList<>();
        for (int i = 0; i < numLoops; ++i) {
            List<XYZPoint> loop = new ArrayList<>();
            unflattenLoopFromBuffer(inBuffer, loop);

            loops.add(loop);
        }

        unflattenBoundFromBuffer(inBuffer);

        return polygonFromXyzPoints(loops);
    }

    /**
     * Google's S2 geometry library uses (x, y, z) representation of polygon vertices,
     * But the interface we expose to users is (lat, lng).  This class is the
     * internal representation for vertices.
     */
    static class XYZPoint {
        private final double m_x;
        private final double m_y;
        private final double m_z;

        public static XYZPoint fromGeographyPointValue(GeographyPointValue pt) {
            double latRadians = pt.getLatitude() * (Math.PI / 180);  // AKA phi
            double lngRadians = pt.getLongitude() * (Math.PI / 180); // AKA theta

            double cosPhi = Math.cos(latRadians);
            double x = Math.cos(lngRadians) * cosPhi;
            double y = Math.sin(lngRadians) * cosPhi;
            double z = Math.sin(latRadians);

            return new XYZPoint(x, y, z);
        }

        public XYZPoint(double x, double y, double z) {
            m_x = x;
            m_y = y;
            m_z = z;
        }

        public double x() {
            return m_x;
        }

        public double y() {
            return m_y;
        }

        public double z() {
            return m_z;
        }

        public GeographyPointValue toGeographyPointValue() {
            double latRadians = Math.atan2(m_z, Math.sqrt(m_x * m_x + m_y * m_y));
            double lngRadians = Math.atan2(m_y, m_x);

            double latDegrees = latRadians * (180 / Math.PI);
            double lngDegrees = lngRadians * (180 / Math.PI);
            return new GeographyPointValue(lngDegrees, latDegrees);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof XYZPoint)) {
                return false;
            }

            XYZPoint compareTo = (XYZPoint) other;

            if(m_x == compareTo.x() && m_y == compareTo.y() && m_z == compareTo.z()) {
                return true;
            }

            return false;
        }

        @Override
        public String toString() {
            return toGeographyPointValue().toString();
        }
    }


    private GeographyValue() {
        m_loops = null;
    }

    static private GeographyValue polygonFromXyzPoints(List<List<XYZPoint>> loops) {

        if (loops == null || loops.size() < 1) {
            throw new IllegalArgumentException("GeographyValue must be instantiated with at least one loop");
        }

        GeographyValue geog = new GeographyValue();
        geog.m_loops = loops;
        return geog;
    }

    private static long boundLengthInBytes() {
        //     1 byte   for encoding version
        //    32 bytes  for lat min, lat max, lng min, lng max as doubles
        return 33;
    }

    private static long loopOverheadInBytes() {
        //   1 byte     for encoding version
        //   4 bytes    for number of vertices
        //   number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        //   1 byte     for origin_inside_
        //   4 bytes    for depth_
        //   length of bound
        return 10 + boundLengthInBytes();
    }

    private static long loopLengthInBytes(long numberOfVertices) {
        return loopOverheadInBytes() + (numberOfVertices * 24);
    }

    private static void flattenEmptyBoundToBuffer(ByteBuffer buf) {
        buf.put(INCOMPLETE_ENCODING_FROM_JAVA); // for encoding version
        buf.putDouble(GeographyPointValue.NULL_COORD);
        buf.putDouble(GeographyPointValue.NULL_COORD);
        buf.putDouble(GeographyPointValue.NULL_COORD);
        buf.putDouble(GeographyPointValue.NULL_COORD);
    }

    private static void serializeEmptyBound(DataOutput output) throws IOException {
        output.write(INCOMPLETE_ENCODING_FROM_JAVA); // for encoding version
        output.writeDouble(GeographyPointValue.NULL_COORD);
        output.writeDouble(GeographyPointValue.NULL_COORD);
        output.writeDouble(GeographyPointValue.NULL_COORD);
        output.writeDouble(GeographyPointValue.NULL_COORD);
    }

    private static void flattenLoopToBuffer(List<XYZPoint> loop, int depth, ByteBuffer buf) {
        //   1 byte     for encoding version
        //   4 bytes    for number of vertices
        //   number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        //   1 byte     for origin_inside_
        //   4 bytes    for depth_
        //   length of bound
        buf.put(INCOMPLETE_ENCODING_FROM_JAVA);
        buf.putInt(loop.size());
        for (XYZPoint xyz : loop) {
            buf.putDouble(xyz.x());
            buf.putDouble(xyz.y());
            buf.putDouble(xyz.z());
        }

        buf.put((byte)0); // origin_inside_
        buf.putInt(depth);// depth
        flattenEmptyBoundToBuffer(buf);
    }

    private static void serializeLoop(List<XYZPoint> loop, int depth, DataOutput output) throws IOException {
        // 1 byte for encoding version
        // 4 bytes for number of vertices
        // number of vertices * 8 * 3 bytes for vertices as XYZPoints
        // 1 byte for origin_inside_
        // 4 bytes for depth_
        // length of bound
        output.write(INCOMPLETE_ENCODING_FROM_JAVA);
        output.writeInt(loop.size());
        for (XYZPoint xyz : loop) {
            output.writeDouble(xyz.x());
            output.writeDouble(xyz.y());
            output.writeDouble(xyz.z());
        }

        output.write((byte) 0); // origin_inside_
        output.writeInt(depth);// depth
        serializeEmptyBound(output);
    }

    private static void unflattenBoundFromBuffer(ByteBuffer inBuffer) {
        inBuffer.get(); // for encoding version
        inBuffer.getDouble();
        inBuffer.getDouble();
        inBuffer.getDouble();
        inBuffer.getDouble();
    }

    private static int unflattenLoopFromBuffer(ByteBuffer inBuffer, List<XYZPoint> loop) {
        //   1 byte     for encoding version
        //   4 bytes    for number of vertices
        //   number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        //   1 byte     for origin_inside_
        //   4 bytes    for depth_
        //   length of bound

        inBuffer.get(); // encoding version
        int numVertices = inBuffer.getInt();
        for (int i = 0; i < numVertices; ++i) {
            double x = inBuffer.getDouble();
            double y = inBuffer.getDouble();
            double z = inBuffer.getDouble();
            loop.add(new XYZPoint(x, y, z));
        }

        inBuffer.get(); // origin_inside_
        int depth = inBuffer.getInt(); // depth
        unflattenBoundFromBuffer(inBuffer);
        return depth;
    }



    /**
     * A helper function to validate the loop structure
     * If loop is invalid, it generates IllegalArgumentException exception
     */

    private static <T> void diagnoseLoop(List<T> loop, String excpMsgPrf) throws IllegalArgumentException {
        if (loop == null) {
            throw new IllegalArgumentException(excpMsgPrf + "a polygon must contain at least one ring " +
                    "(with each ring at least 4 points, including repeated closing vertex)");
        }

        // 4 vertices = 3 unique vertices for polygon + 1 end point which is same as start point
        if (loop.size() < 4) {
            throw new IllegalArgumentException(excpMsgPrf + "a polygon ring must contain at least 4 points " +
                    "(including repeated closing vertex)");
        }

        // check if the end points of the loop are equal
        if (loop.get(0).equals(loop.get(loop.size() - 1)) == false) {
            throw new IllegalArgumentException(excpMsgPrf
                                                + "closing points of ring are not equal: \""
                                                + loop.get(0).toString()
                                                + "\" != \""
                                                + loop.get(loop.size()-1).toString()
                                                + "\"");
        }
    }

    /**
     * A helper method to parse WKT and produce a list of polygon loops.
     * Anything more complicated than this and we probably want a dedicated parser.
     *
     * Note that we assume that the vertices of the first loop are in counter-clockwise
     * order, and that subsequent loops are in clockwise order.  This is the OGC format's
     * definition.  When we send these to the EE we need to put them all into counter-clockwise
     * order.  So, we need to reverse the order of all but the first loop.
     */
    private static List<List<XYZPoint>> loopsFromWkt(String wkt) throws IllegalArgumentException {
        final String msgPrefix = "Improperly formatted WKT for polygon: ";

        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(wkt));
        tokenizer.lowerCaseMode(true);
        tokenizer.eolIsSignificant(false);

        List<XYZPoint> currentLoop = null;
        List<List<XYZPoint>> loops = new ArrayList<>();
        boolean is_shell = true;
        try {
            int token = tokenizer.nextToken();
            if (token != StreamTokenizer.TT_WORD
                    || ! tokenizer.sval.equals("polygon")) {
                throw new IllegalArgumentException(msgPrefix + "expected WKT to start with POLYGON");
            }

            token = tokenizer.nextToken();
            if (token != '(') {
                throw new IllegalArgumentException(msgPrefix + "expected left parenthesis after POLYGON");
            }

            boolean polygonOpen = true;

            while (polygonOpen) {

                token = tokenizer.nextToken();
                switch (token) {
                case '(':
                    if (currentLoop != null) {
                        throw new IllegalArgumentException(msgPrefix + "missing closing parenthesis");
                    }
                    currentLoop = new ArrayList<>();
                    break;
                case StreamTokenizer.TT_NUMBER:
                    if (currentLoop == null) {
                        throw new IllegalArgumentException(msgPrefix + "missing opening parenthesis");
                    }

                    double lng = tokenizer.nval;
                    token = tokenizer.nextToken();
                    if (token != StreamTokenizer.TT_NUMBER) {
                        throw new IllegalArgumentException(msgPrefix + "missing latitude in long lat pair");
                    }
                    double lat = tokenizer.nval;
                    currentLoop.add(XYZPoint.fromGeographyPointValue(new GeographyPointValue(lng, lat)));

                    token = tokenizer.nextToken();
                    if (token != ',') {
                        if (token != ')') {
                            throw new IllegalArgumentException(msgPrefix + "missing comma between long lat pairs");
                        }
                        tokenizer.pushBack();
                    }
                    break;
                case ')':
                    // perform basic validation of loop
                    diagnoseLoop(currentLoop, msgPrefix);
                    // Following the OGC standard, the first loop should be CCW, and subsequent loops
                    // should be CW.  But we will be building the S2 polygon here,
                    // and S2 wants everything to be CCW.  So, we need to
                    // reverse all but the first loop.
                    //
                    // Note also that we don't want to touch the vertex at index 0, and we want
                    // to remove the vertex at index currentLoop.size() - 1.  We want to hold the first
                    // vertex invariant.  The vertex at currentLoop.size() - 1 should be a duplicate
                    // of the vertex at index 0, and should be removed before pushing it into the
                    // list of loops.
                    //
                    // We are also allowed to swap these out, because they have been
                    // created and are owned by us.
                    //
                    currentLoop.remove(currentLoop.size() - 1);
                    if (!is_shell) {
                        for (int fidx = 1, lidx = currentLoop.size() - 1; fidx < lidx; ++fidx, --lidx) {
                            Collections.swap(currentLoop, fidx, lidx);
                        }
                    }
                    is_shell = false;
                    loops.add(currentLoop);
                    currentLoop = null;

                    token = tokenizer.nextToken();
                    if (token == ')') {
                        polygonOpen = false;
                    }
                    else if (token != ',') {
                        throw new IllegalArgumentException(msgPrefix + "unrecognized token in WKT: " + Character.toString((char)token));
                    }

                    break;

                case StreamTokenizer.TT_EOF:
                    throw new IllegalArgumentException(msgPrefix + "premature end of input");

                default:
                    throw new IllegalArgumentException(msgPrefix + "unrecognized token in WKT: " + Character.toString((char)token));
                }
            }

            token = tokenizer.nextToken();
            if (token != StreamTokenizer.TT_EOF) {
                throw new IllegalArgumentException(msgPrefix + "unrecognized input after WKT");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(msgPrefix + "error tokenizing string");
        }

        return loops;
    }

    /**
     * Create a new GeographyValue which is offset from this one
     * by the given point.  The latitude and longitude values
     * stay in range because we are using the normalizing operations
     * in GeographyPointValue.
     *
     * @param offset  The point by which to translate vertices in this
     * @return  The resulting GeographyValue.
     */
    @Deprecated
    public GeographyValue add(GeographyPointValue offset) {
        List<List<GeographyPointValue>> newLoops = new ArrayList<>();
        for (List<XYZPoint> oneLoop : m_loops) {
            List<GeographyPointValue> loop = new ArrayList<>();
            for (XYZPoint p : oneLoop) {
                loop.add(p.toGeographyPointValue().add(offset));
            }
            loop.add(oneLoop.get(0).toGeographyPointValue().add(offset));
            newLoops.add(loop);
        }
        return new GeographyValue(newLoops, true);
    }
}
