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
import java.util.List;

/**
 * This class is used to values in GEOGRAPHY columns in database tables.
 * For now, this means polygons, but may include other kinds of geospatial
 * types in the future (linestring, etc)
 */
public class GeographyValue {

    private List<List<XYZPoint>> m_loops;

    /**
     * Create a polygon from a list of rings.  Each ring is a list of points:
     *   - The first ring in the list is the outer ring
     *   - Subsequent rings should be inside of the outer ring and represent
     *     "holes" in the polygon
     *   - Each ring should have its vertices listed in counter-clockwise order,
     *     so that the area inside the ring is on the left side of the line segments
     *     formed by adjacent vertices.
     *
     * @param loops
     */
    public GeographyValue(List<List<PointType>> loops) {
        if (loops == null || loops.size() < 1) {
            throw new IllegalArgumentException("GeographyValue must be instantiated with at least one loop");
        }

        m_loops = new ArrayList<List<XYZPoint>>();
        for (List<PointType> loop : loops) {
            List<XYZPoint> oneLoop = new ArrayList<XYZPoint>();
            for (int i = 0; i < loop.size(); ++i) {
                oneLoop.add(XYZPoint.fromPointType(loop.get(i)));
            }
            diagnoseLoop(oneLoop, "Invalid loop for GeographyValue: ");
            oneLoop.remove(oneLoop.size() - 1);
            m_loops.add(oneLoop);
        }
    }

    /**
     * Create a polygon given the well-known text representation.  As with the
     * above constructor, the outer ring should be first and vertices should be
     * listed on counter-clockwise order.
     *
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

    public static GeographyValue geographyValueFromText(String text) {
        return new GeographyValue(text);
    }

    /**
     * Gets the loops that make up the polygon, with the outer loop first.
     * @return  The loops in the polygon as a list of a list of points
     */
    public List<List<PointType>> getLoops() {
        List<List<PointType>> llLoops = new ArrayList<List<PointType>>();

        for (List<XYZPoint> xyzLoop : m_loops) {
            List<PointType> llLoop = new ArrayList<PointType>();
            for (XYZPoint xyz : xyzLoop) {
                llLoop.add(xyz.toPointType());
            }
            llLoops.add(llLoop);
        }
        return llLoops;
    }

    /**
     * Print out this polygon in WKT format.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("POLYGON(");

        boolean isFirstLoop = true;
        for (List<XYZPoint> loop : m_loops) {
            if (isFirstLoop) {
                isFirstLoop = false;
            }
            else {
                sb.append(", ");
            }

            sb.append("(");
            for (XYZPoint xyz : loop) {
                sb.append(xyz.toPointType().formatLatLng());
                sb.append(", ");
            }

            // Repeat the first vertex to close the loop as WKT requires.
            sb.append(loop.get(0).toPointType().formatLatLng());
            sb.append(")");
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

        public static XYZPoint fromPointType(PointType pt) {
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

        public PointType toPointType() {
            double latRadians = Math.atan2(m_z, Math.sqrt(m_x * m_x + m_y * m_y));
            double lngRadians = Math.atan2(m_y, m_x);

            double latDegrees = latRadians * (180 / Math.PI);
            double lngDegrees = lngRadians * (180 / Math.PI);
            return new PointType(latDegrees, lngDegrees);
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
        buf.putDouble(PointType.NULL_COORD);
        buf.putDouble(PointType.NULL_COORD);
        buf.putDouble(PointType.NULL_COORD);
        buf.putDouble(PointType.NULL_COORD);
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

    private static void diagnoseLoop(List<XYZPoint> loop, String excpMsgPrf) throws IllegalArgumentException {
        if (loop == null) {
            throw new IllegalArgumentException(excpMsgPrf + "polygon should contain atleast one loop, " +
                    "with each loop containing minimum of 4 vertices - start and end vertices being equal");
        }

        // 4 vertices = 3 unique vertices for polygon + 1 end point which is same as start point
        if (loop.size() < 4) {
            throw new IllegalArgumentException(excpMsgPrf + "each loop in polygon should have 4 vertices, with start and end vertices equal");
        }

        // check if the end points of the loop are equal
        if (loop.get(0).equals(loop.get(loop.size() - 1)) == false) {
            throw new IllegalArgumentException(excpMsgPrf + "start and end vertices of loop are not equal");
        }
    }

    /**
     * A helper method to parse WKT and produce a list of polygon loops.
     * Anything more complicated than this and we probably want a dedicated parser.
     */
    private static List<List<XYZPoint>> loopsFromWkt(String wkt) throws IllegalArgumentException {
        final String msgPrefix = "Improperly formatted WKT for polygon: ";

        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(wkt));
        tokenizer.lowerCaseMode(true);
        tokenizer.eolIsSignificant(false);

        List<XYZPoint> currentLoop = null;
        List<List<XYZPoint>> loops = new ArrayList<List<XYZPoint>>();
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

                    double lat = tokenizer.nval;
                    token = tokenizer.nextToken();
                    if (token != StreamTokenizer.TT_NUMBER) {
                        throw new IllegalArgumentException(msgPrefix + "missing longitude in lat long pair");
                    }
                    double lng = tokenizer.nval;
                    currentLoop.add(XYZPoint.fromPointType(new PointType(lat, lng)));

                    token = tokenizer.nextToken();
                    if (token != ',') {
                        if (token != ')') {
                            throw new IllegalArgumentException(msgPrefix + "missing comma between lat long pairs");
                        }
                        tokenizer.pushBack();
                    }
                    break;
                case ')':
                    // perform basic validation of loop
                    diagnoseLoop(currentLoop, msgPrefix);
                    currentLoop.remove(currentLoop.size() - 1);

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

}
