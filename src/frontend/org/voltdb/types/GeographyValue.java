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

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class is used to values in GEOGRAPHY columns in database tables.
 * For now, this means polygons, but may include other kinds of geospatial
 * types in the future (linestring, etc)
 */
public class GeographyValue {
    //
    // This is a list of loops.  Each loop must be in
    // S2Loop format.  That is to say, it must have type XYPPoint
    // type, it must be in counter-clockwise order and it must not
    // be closed.  All loops, even holes, are CCW.
    //
    private List<List<XYZPoint>> m_loops;

     /**
     * Create a polygon from a list of rings.  Each ring is a list of points:
     *   - The first ring in the list is the outer ring, also known as the
     *     shell.
     *   - Subsequent rings should be inside of the outer ring and represent
     *     "holes" in the polygon.
     *   - Each shell should have its vertices listed in counter-clockwise order,
     *     so that the area inside the ring is on the left side of the line segments
     *     formed by adjacent vertices.
     *   - Each hole, or inner ring, should have its vertices listed in clockwise
     *     order, so that the area inside the ring is on the right side of the
     *     line segments formed by adjacent vertices.
     *
     * Note that this is the same as the order expected by the OGC standard's
     * Well Known Text format.
     *
     * Note also that the loops here are GeographyPointValues, and that they
     * are closed loops.  That is to say, the first vertex and the last
     * vertex must be equal.
     *
     * @param loops
     */
     public GeographyValue(List<List<GeographyPointValue>> loops) {
         if (loops == null || loops.size() < 1) {
             throw new IllegalArgumentException("GeographyValue must be instantiated with at least one loop");
         }
         // Note that we need to reverse all but the
         // first loop, since the EE wants them all in CCW order,
         // and the OGC order for holes is CW.
         //
         m_loops = new ArrayList<List<XYZPoint>>();
         boolean firstLoop = true;
         for (List<GeographyPointValue> loop : loops) {
             diagnoseLoop(loop, "Invalid loop for GeographyValue: ");
             List<XYZPoint> oneLoop = new ArrayList<XYZPoint>();
             int startIdx;
             int endIdx;
             int delta;
             if (firstLoop) {
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
     * Create a GeometryValue object from an OGC Well Known Text format string.
     * The format, which is different from the raw loop constructor above, is
     * as follows.
     * <ol>
     *   <li>Each polygon is a sequence of loops.</li>
     *   <li>Each loop is either a shell or a hole.  There is exactly
     *       one shell, but there may be zero or more holes.</li>
     *   <li>The first loop in the sequence is the only shell.  Its vertices
     *       should be given in counterclockwise order</li>
     *   <li>All subsequent loops, if there are any, are holes.  Their vertices
     *       should be given in <em>clockwise</em> order.  Note that this
     *       is the opposite of the raw loop constructor from above.
     * @param wkt
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
     * This is a factory function to create a GeometryValue object
     * from OGC's Well Known Text strings.  The format is given
     * in the documentation for {@link GeographyValue.GeographyValue(String)}.
     *
     * @param text
     * @return
     */
    public static GeographyValue fromText(String text) {
        return new GeographyValue(text);
    }

    /**
     * Return the list of loops of a polygon.  The list has the same
     * values as the list of loops used to construct the polygon, or
     * the sequence of WKT loops used to construct the polygon.
     *
     * @return A list of loops.
     */
    public List<List<GeographyPointValue>> getLoops() {
        /*
         * Gets the loops that make up the polygon, with the outer loop first.
         * Note that we need to convert from XYZPoint to GeographyPointValue.
         *
         * Include the loop back to the first vertex.  Also, since WKT wants
         * holes oriented Clockwise and S2 wants everything oriented CounterClockWise,
         * reverse the order of holes.  We take care to leave the first vertex
         * the same.
         */
        List<List<GeographyPointValue>> llLoops = new ArrayList<List<GeographyPointValue>>();

        boolean isShell = true;
        for (List<XYZPoint> xyzLoop : m_loops) {
            List<GeographyPointValue> llLoop = new ArrayList<GeographyPointValue>();
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
     * Print out this polygon in WKT format.  Use 12 digits of precision.
     */
    @Override
    public String toString() {
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
    public static final int DEFAULT_LENGTH = 32768;

    /**
     * Return the number of bytes in the serialization for this polygon
     * (not including the 4-byte length prefix that precedes variable-length types).
     *  */
    public int getLengthInBytes() {
        long length = 7;
        for (List<XYZPoint> loop : m_loops) {
            length += loopLengthInBytes(loop.size());
        }

        length += boundLengthInBytes();

        return (int)length;
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
        for (List<XYZPoint> loop : m_loops) {
            flattenLoopToBuffer(loop, buf);
        }
        flattenEmptyBoundToBuffer(buf);
    }

    /**
     * Deserialize a GeographyValue from a ByteBuffer.
     * (Assumes that the 4-byte length prefix has already been deserialized, and that
     * offset points to the start of data just after the prefix.)
     * @param inBuffer
     * @param offset
     * @return a new GeographyValue
     */
    public static GeographyValue unflattenFromBuffer(ByteBuffer inBuffer, int offset) {
        int origPos = inBuffer.position();
        inBuffer.position(offset);
        GeographyValue gv = unflattenFromBuffer(inBuffer);
        inBuffer.position(origPos);
        return gv;
    }

    /**
     * Deserialize a GeographyValue from a ByteBuffer.
     * (Assumes that the 4-byte length prefix has already been deserialized.)
     * @param inBuffer
     * @return a new GeographyValue
     */
    public static GeographyValue unflattenFromBuffer(ByteBuffer inBuffer) {
        byte version = inBuffer.get(); // encoding version
        inBuffer.get(); // owns loops
        inBuffer.get(); // has holes
        int numLoops = inBuffer.getInt();
        List<List<XYZPoint>> loops = new ArrayList<List<XYZPoint>>();
        int indexOfOuterRing = 0;
        for (int i = 0; i < numLoops; ++i) {
            List<XYZPoint> loop = new ArrayList<XYZPoint>();
            int depth = unflattenLoopFromBuffer(inBuffer, loop);
            if (depth == 0) {
                indexOfOuterRing = i;
            }

            loops.add(loop);
        }

        // S2 will order loops in depth-first order, which will leave the outer ring last.
        // Make it first so it looks right when converted back to WKT.
        if (version == COMPLETE_ENCODING && indexOfOuterRing != 0) {
                    List<XYZPoint> outerRing = loops.get(indexOfOuterRing);
                    loops.set(indexOfOuterRing, loops.get(0));
                    loops.set(0, outerRing);
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

    private static long loopLengthInBytes(long numberOfVertices) {
        //   1 byte     for encoding version
        //   4 bytes    for number of vertices
        //   number of vertices * 8 * 3  bytes  for vertices as XYZPoints
        //   1 byte     for origin_inside_
        //   4 bytes    for depth_
        //   length of bound
        return 5 + (numberOfVertices * 24) + 5 + boundLengthInBytes();
    }

    private static void flattenEmptyBoundToBuffer(ByteBuffer buf) {
        buf.put(INCOMPLETE_ENCODING_FROM_JAVA); // for encoding version
        buf.putDouble(GeographyPointValue.NULL_COORD);
        buf.putDouble(GeographyPointValue.NULL_COORD);
        buf.putDouble(GeographyPointValue.NULL_COORD);
        buf.putDouble(GeographyPointValue.NULL_COORD);
    }

    private static void flattenLoopToBuffer(List<XYZPoint> loop, ByteBuffer buf) {
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
        buf.putInt(0); // depth_
        flattenEmptyBoundToBuffer(buf);
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
        List<List<XYZPoint>> loops = new ArrayList<List<XYZPoint>>();
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
                    currentLoop = new ArrayList<XYZPoint>();
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
     * @param offset
     * @return
     */
    public GeographyValue add(GeographyPointValue offset) {
        List<List<GeographyPointValue>> newLoops = new ArrayList<List<GeographyPointValue>>();
        for (List<XYZPoint> oneLoop : m_loops) {
            List<GeographyPointValue> loop = new ArrayList<GeographyPointValue>();
            for (XYZPoint p : oneLoop) {
                loop.add(p.toGeographyPointValue().add(offset));
            }
            loop.add(oneLoop.get(0).toGeographyPointValue().add(offset));
            newLoops.add(loop);
        }
        return new GeographyValue(newLoops);
    }
}
