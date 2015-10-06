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

    List<List<PointType>> m_loops;

    /**
     * Create a polygon from at least an outer ring, and possibly inner rings.
     * Some geometry libraries prefer the outer ring to list the vertices in
     * counter-clockwise (CCW) order, and the inner rings to be in CW order.
     * At some point we'll need to hash out these requirements and where we
     * validate them.
     *
     * @param loops
     */
    public GeographyValue(List<List<PointType>> loops) {
        if (loops == null || loops.size() < 1) {
            throw new IllegalArgumentException("GeographyValue must be instantiated with at least one loop");
        }

        m_loops = loops;
    }

    /**
     * Create a polygon given the well-known text representation.
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
     * Gets the loops that make up the polygon, with the outer loop first.
     * @return  The loops in the polygon as a list of a list of points
     */
    public List<List<PointType>> getLoops() {
        return m_loops;
    }

    /**
     * Print out this polygon in WKT format.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("POLYGON(");

        boolean isFirstLoop = true;
        for (List<PointType> loop : m_loops) {
            if (isFirstLoop) {
                isFirstLoop = false;
            }
            else {
                sb.append(", ");
            }

            sb.append("(");
            boolean isFirstVertex = true;
            for (PointType pt : loop) {
                if (isFirstVertex) {
                    isFirstVertex = false;
                }
                else {
                    sb.append(", ");
                }

                sb.append(pt.getLatitude() + " " + pt.getLongitude());
            }

            sb.append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Return the number of bytes in the serialization for this polygon
     * (not including the 4-byte length prefix that precedes variable-length types).
     *  */
    public int getLengthInBytes() {
        int length = 4; // number of loops prefix
        for (List<PointType> loop : m_loops) {
            length += 4; // number of vertices prefix;
            length += (loop.size() * PointType.getLengthInBytes());
        }

        return length;
    }

    /**
     * Serialize this object to a ByteBuffer.
     * (Assumes that the 4-byte length prefix for variable-length data
     * has already been serialized.)
     *
     * Here's the serialization format for polygons:
     *   The number of loops in the polygon as a 4-byte int
     *   For each loop:
     *     The number of vertices in the polygon as a 4-byte int
     *     For each vertex in the loop, a serialization of the PointType object
     *
     * @param buf  The ByteBuffer into which the serialization will be placed.
     */
    public void flattenToBuffer(ByteBuffer buf) {
        buf.putInt(m_loops.size());
        for (List<PointType> loop : m_loops) {
            buf.putInt(loop.size());
            for (PointType pt : loop) {
                pt.flattenToBuffer(buf);
            }
        }
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
        int numLoops = inBuffer.getInt(offset);
        offset += 4;

        List<List<PointType>> loops = new ArrayList<List<PointType>>();
        for (int i = 0; i < numLoops; ++i){
            List<PointType> loop = new ArrayList<PointType>();
            int numVertices = inBuffer.getInt(offset);
            offset += 4;
            for (int j = 0; j < numVertices; ++j) {
                PointType pt = PointType.unflattenFromBuffer(inBuffer, offset);
                offset += PointType.getLengthInBytes();
                loop.add(pt);
            }
            loops.add(loop);
        }
        return new GeographyValue(loops);
    }

    /**
     * Deserialize a GeographyValue from a ByteBuffer.
     * (Assumes that the 4-byte length prefix has already been deserialized.)
     * @param inBuffer
     * @return a new GeographyValue
     */
    public static GeographyValue unflattenFromBuffer(ByteBuffer inBuffer) {
        int numLoops = inBuffer.getInt();

        List<List<PointType>> loops = new ArrayList<List<PointType>>();
        for (int i = 0; i < numLoops; ++i){
            List<PointType> loop = new ArrayList<PointType>();
            int numVertices = inBuffer.getInt();
            for (int j = 0; j < numVertices; ++j) {
                PointType pt = PointType.unflattenFromBuffer(inBuffer);
                loop.add(pt);
            }
            loops.add(loop);
        }
        return new GeographyValue(loops);
    }

    /**
     * A helper method to parse WKT and produce a list of polygon loops.
     * Anything more complicated than this and we probably want a dedicated parser.
     */
    private static List<List<PointType>> loopsFromWkt(String wkt) throws IllegalArgumentException {
        final String msgPrefix = "Improperly formatted WKT for polygon: ";

        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(wkt));
        tokenizer.lowerCaseMode(true);
        tokenizer.eolIsSignificant(false);

        List<PointType> currentLoop = null;
        List<List<PointType>> loops = new ArrayList<List<PointType>>();
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
                    currentLoop = new ArrayList<PointType>();
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
                    currentLoop.add(new PointType(lat, lng));

                    token = tokenizer.nextToken();
                    if (token != ',') {
                        if (token != ')') {
                            throw new IllegalArgumentException(msgPrefix + "missing comma between lat long pairs");
                        }
                        tokenizer.pushBack();
                    }
                    break;
                case ')':
                    if (currentLoop == null) {
                        throw new IllegalArgumentException(msgPrefix + "missing opening parenthesis");
                    }
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
