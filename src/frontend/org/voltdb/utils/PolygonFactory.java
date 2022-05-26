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
package org.voltdb.utils;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

/**
 * A class to create polygons.
 */
public class PolygonFactory {
    /**
     * Create a regular convex polygon, with an optional hole.
     *
     * Note that the resulting polygon will be symmetric around any line
     * through the center and a vertex.  Consequently, the centroid of such
     * a polygon must be the center of the polygon.
     *
     * @param center The center of the polygon.
     * @param firstVertex The coordinates of the first vertex.
     * @param numVertices The number of vertices.
     * @param sizeOfHole If this is positive, we also create a hole whose vertices are
     *                   at the same angle from the polygon's center, but whose distance
     *                   is scaled by sizeOfHole.  This value must be in the range [0,1).
     * @return
     */
    @Deprecated
    public static GeographyValue CreateRegularConvex(
            GeographyPointValue center,
            GeographyPointValue firstVertex,
            int numVertices,
            double sizeOfHole) {
        assert(0 <= sizeOfHole && sizeOfHole < 1.0);
        double phi = 360.0/numVertices;
        GeographyPointValue holeFirstVertex = null;
        if (sizeOfHole > 0) {
            holeFirstVertex = firstVertex.scale(center, sizeOfHole);
        }
        List<GeographyPointValue> oneLoop = new ArrayList<>();
        List<GeographyPointValue> hole = (sizeOfHole < 0 ? null : new ArrayList<>());
        // We will add the nth point at angle n*phi.  For shells
        // We want to add points in a CCW order, so phi must be
        // a positive angle.  For holes we want to add in a CW order,
        // so phy mist be a negative angle.
        for (int idx = 0; idx < numVertices; idx += 1) {
            int holeIdx = numVertices-idx;
            oneLoop.add(firstVertex.rotate(idx*phi, center));
            if (sizeOfHole > 0) {
                hole.add(holeFirstVertex.rotate(-(holeIdx*phi), center));
            }
        }
        // Add the closing vertices.
        oneLoop.add(firstVertex);
        if (sizeOfHole > 0) {
            hole.add(holeFirstVertex);
        }
        List<List<GeographyPointValue>> loops = new ArrayList<>();
        loops.add(oneLoop);
        if (sizeOfHole > 0) {
            loops.add(hole);
        }
        return new GeographyValue(loops);
    }

    /**
     * Create a star-shaped polygon with an optional hole.  This polygon will have
     * numPointsInStar outer points, and numPointsInStar inner points.  For each
     * outer point, OP, distance(center, OP) = distance(center, firstVertex).  For
     * each inner point, IP, distance(center, IP) = ratioOfPointLength*distance(center, firstVertex).
     *
     * If sizeOfHole is positive, then there is a hole with inner and outer vertices, as
     * with the exterior shell.  For each hole exterior point, HEP,
     *     distance(center, HEP) = sizeOfHole*distance(center, firstVertex).  For each
     * hole interior point, HIP,
     *     distance(center, HIP) = sizeOfHole*rationOfPointLength*distance(center, firstVertex).
     * So, the hole is equal to the exterior shell scaled by the number sizeOfHole.
     *
     * Note that this polygon will be symmetric around any line through the center and
     * an outer or inner point.  Consequently, the centroid of the generated polygon
     * must be the center.
     *
     * @param center The center of the polygon.
     * @param firstVertex The first vertex.
     * @param numPointsInStar The number of exterior points in the star.
     * @param ratioOfPointLength The outer/inner scale factor.  This must be in the range (0,1].
     * @param sizeOfHole The scale factor for the hole.  This must be in the range [0,1).
     * @return
     * @throws IllegalArgumentException
     */
    @Deprecated
    public static GeographyValue CreateStar(
            GeographyPointValue center,
            GeographyPointValue firstVertex,
            int numPointsInStar,
            double ratioOfPointLength, // pass in 1.0 for a polygon with twice the number of vertices, or .1 for a very jagged star
            double sizeOfHole) throws IllegalArgumentException {
        // I don't think a 1 or 2 pointed star is possible.
        if (numPointsInStar < 3) {
            throw new IllegalArgumentException("Star polygons must have 3 or more points.");
        }

        if (sizeOfHole < 0 || 1.0 <= sizeOfHole) {
            throw new IllegalArgumentException("Star polygon hole size must be in the range [0.0, 1.0)");
        }
        if (ratioOfPointLength <= 0 || 1.0 < ratioOfPointLength) {
            throw new IllegalArgumentException("Star polygon external/internal radius ration must be in the range (0.0, 1.0]");
        }
        // We will add the nth point at angle n*phi.  We want to
        // add points in a counter clockwise order, so phi must be
        // a positive angle.  We will have twice as many vertices
        // as points.
        double phi = 360.0/(2*numPointsInStar);
        GeographyPointValue innerFirstVertex = firstVertex.scale(center, ratioOfPointLength);
        GeographyPointValue holeFirstVertex = null;
        GeographyPointValue innerHoleFirstVertex = null;

        // Calculate hole radii.
        if (sizeOfHole > 0) {
            holeFirstVertex = firstVertex.scale(center, sizeOfHole);
            innerHoleFirstVertex = firstVertex.scale(center, sizeOfHole * ratioOfPointLength);
        }
        // The outer and inner radii go here. We will
        // index into this array using the last bit of
        // an integer loop index.
        GeographyPointValue firstVertices[] = {
                firstVertex,
                innerFirstVertex
        };
        // We use the same trick here.
        GeographyPointValue holeFirstVertices[] = {
                holeFirstVertex,
                innerHoleFirstVertex
        };
        //
        // We have to add all shells in counter clockwise order, and all
        // holes in clockwise order.  This amounts to rotating the shell
        // generator vector by -phi and the hole generator vector by phi.
        //
        List<GeographyPointValue> outerLoop = new ArrayList<>();
        List<GeographyPointValue> holeLoop = null;
        if (sizeOfHole > 0) {
            holeLoop = new ArrayList<>();
        }
        for (int idx = 0; idx < 2*numPointsInStar; idx += 1) {
            GeographyPointValue vert = null;
            GeographyPointValue holeVert = null;
            // Even vertices are the points.
            // Odd vertices are the valleys, if that's the right
            // term.
            vert = firstVertices[idx % 2];
            holeVert = holeFirstVertices[idx % 2];
            outerLoop.add(vert.rotate(idx*phi, center));
            if (sizeOfHole > 0) {
                holeLoop.add(holeVert.rotate(-(idx*phi), center));
            }
        }
        outerLoop.add(outerLoop.get(0));
        if (sizeOfHole > 0) {
            holeLoop.add(holeLoop.get(0));
        }
        List<List<GeographyPointValue>> loops = new ArrayList<>();
        loops.add(outerLoop);
        if (sizeOfHole > 0) {
            loops.add(holeLoop);
        }
        return new GeographyValue(loops);
    }

    /**
     * Reverse all the loops in a polygon.  Don't change the
     * order of the loops, just reverse each loop.
     *
     * This is useful for testing a malformed polygon.
     *
     * @param goodPolygon
     * @return
     */
    @Deprecated
    public static GeographyValue reverseLoops(GeographyValue goodPolygon) {
        List<List<GeographyPointValue>> newLoops = new ArrayList<>();
        List<List<GeographyPointValue>> oldLoops = goodPolygon.getRings();
        for (List<GeographyPointValue> loop : oldLoops) {
            // Copy loop, but reverse the points.
            List<GeographyPointValue> newLoop = new ArrayList<>();
            // Leave the first and last one fixed, but copy
            // all the others from the end.
            newLoop.add(loop.get(0));
            for (int idx = loop.size() - 2; idx > 1; idx -= 1) {
                newLoop.add(loop.get(idx));
            }
            newLoops.add(newLoop);
        }
        return new GeographyValue(newLoops);
    }
}
