/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.utils;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

import junit.framework.TestCase;

public class TestPolygonFactory extends TestCase {
    static final GeographyPointValue origin = new GeographyPointValue(0.0, 0.0);
    static final GeographyPointValue x = new GeographyPointValue(1.0, 0.0);
    static final GeographyPointValue y = new GeographyPointValue(0.0, 1.0);
    static final double CENTER_SHRINK = 0.3;

    public void testRegularConvexPolygon() throws Exception {
        // Test a triangle.
        GeographyValue pt3 = PolygonFactory.CreateRegularConvex(origin, y.mul(20.0), 3, 0);
        String triangle = "POLYGON ((0.0 20.0, -17.320508075689 -10.0, 17.320508075689 -10.0, 0.0 20.0))";
        assertEquals(triangle, pt3.toString());
        // Test a square.
        GeographyValue pt4 = PolygonFactory.CreateRegularConvex(origin, y.mul(20).add(x.mul(20)), 4, 0);
        String square = "POLYGON ((20.0 20.0, -20.0 20.0, -20.0 -20.0, 20.0 -20.0, 20.0 20.0))";
        assertEquals(square, pt4.toString());
        GeographyPointValue offset = x.mul(20).add(y.mul(20));
        GeographyValue pt4plus = pt4.add(offset);
        String squareOff = "POLYGON ((40.0 40.0, 0.0 40.0, 0.0 0.0, 40.0 0.0, 40.0 40.0))";
        assertEquals(squareOff, pt4plus.toString());
        // For n = 3 to 10, generate a regular polygon with n points
        // centered at the origin starting at the given start vertex.
        GeographyPointValue startVertex = x.add(y);
        for (int npts = 3; npts < 10; npts += 1) {
            GeographyValue regularConvex = PolygonFactory.CreateRegularConvex(origin,  startVertex, npts, 0.0);
            List<List<GeographyPointValue>> loops = regularConvex.getRings();
            assertEquals(1, loops.size());
            List<GeographyPointValue> loop = loops.get(0);
            assertEquals(npts + 1, loop.size());
            regularConvex = PolygonFactory.CreateRegularConvex(origin,  startVertex, npts, 0.5);
            loops = regularConvex.getRings();
            assertEquals(2, loops.size());
            assertEquals(npts + 1, loops.get(0).size());
            assertEquals(npts + 1, loops.get(1).size());
        }
    }

    public void testStarPolygon() throws Exception {
        for (int idx = 3; idx < 10; idx += 1) {
            GeographyValue star = PolygonFactory.CreateStar(origin, y.mul(20.0), idx, 0.5, 0.0);
            List<List<GeographyPointValue>> loops = star.getRings();
            assertEquals(1, loops.size());
            List<GeographyPointValue> shell = loops.get(0);
            assertEquals(2*idx+1, shell.size());
            star = PolygonFactory.CreateStar(origin, y.mul(20).add(x.mul(20)), idx, 0.5, 0.1);
            loops = star.getRings();
            assertEquals(2, loops.size());
            shell = loops.get(0);
            List<GeographyPointValue> hole = loops.get(1);
            assertEquals(2*idx+1, shell.size());
            assertEquals(2*idx+1, hole.size());
        }
    }

    /**
     * Create many regular convex polygons.  In returnValue.get(n).get(k) we put an
     * (n+3)-sided polygon with the given center and start vertex, with hole size
     * equal to k/numHoleSizes.  If k == 0 there is no hole.  Note that k ranges between
     * 0 and 4, so k/NumHoleSizes ranges between 0 and (1-1/numHoleSizes).  If k == 0
     * there is no hole.
     *
     * @return
     */
    private static List<List<GeographyValue>> makeRegularConvexPolygons(GeographyPointValue firstCenter,
                                                                 GeographyPointValue firstFirstVertex,
                                                                 int minNumberVertices,
                                                                 int maxNumberVertices,
                                                                 int numHoleSizes,
                                                                 double xmul,
                                                                 double ymul) {
        List<List<GeographyValue>> answer = new ArrayList<>();
        for (int numVertices = minNumberVertices; numVertices <= maxNumberVertices; numVertices += 1) {
            int idx = numVertices - minNumberVertices;
            List<GeographyValue> oneSize = new ArrayList<>();
            // The x coordinate is humHoleSizes*idx.
            GeographyPointValue sCenter = firstCenter.add(x.mul(numHoleSizes*idx));
            for (int hidx = 0; hidx < numHoleSizes; hidx += 1) {
                // The y coordinate is ymul * hidx.
                GeographyPointValue offset = sCenter.add(y.mul(ymul*hidx));
                GeographyPointValue center = firstCenter.add(offset);
                GeographyPointValue firstVertex = firstFirstVertex.add(offset);
                oneSize.add(PolygonFactory.CreateRegularConvex(center, firstVertex, numVertices, (hidx+0.0)/numHoleSizes));
            }
            answer.add(oneSize);
        }
        return answer;
    }

    /**
     * Create many star-like polygons.  In returnValue.get(n).get(s).get(k) we put an
     * n-pointed polygon with the given center and start vertex.  The inner radius is
     * (numIrLevels-s+1)/numIrLevels.  The hole size is k/numHoleSizeLevels.
     * If k == 0, there is no hole.
     *
     * Note that n ranges between minNumPoints and maxNumPoints inclusive,
     * s between 0 numIrLevels-1, k between 0 and numHoleSizeLevels-1.  Since
     * the hole size and inner radius must both be less than 1, and the inner radius
     * must be greater than zero.  The hole size can be zero.
     *
     * @return A 3-dimensional list list of polygons.
     */
    private static List<List<List<GeographyValue>>> makeStarPolygons(GeographyPointValue firstCenter,
                                                                     GeographyPointValue firstFirstVertex,
                                                                     int minNumPoints,
                                                                     int maxNumPoints,
                                                                     int numIRLevels,
                                                                     int numHoleSizeLevels,
                                                                     double xmul,
                                                                     double ymul) {
        List<List<List<GeographyValue>>> answer = new ArrayList<>();
        for (int numSides = minNumPoints; numSides <= maxNumPoints; numSides += 1) {
            int idx = numSides - minNumPoints;
            // The x coordinate is xmul * idx
            GeographyPointValue column = x.mul(xmul*idx);
            List<List<GeographyValue>> oneSize = new ArrayList<>();
            for (int ratioLevel = 0; ratioLevel < numIRLevels; ratioLevel += 1) {
                GeographyPointValue irCenter = column.add(y.mul(numHoleSizeLevels*ymul*ratioLevel));
                List<GeographyValue> oneRadius = new ArrayList<>();
                for (int holeNumber = 0; holeNumber < numHoleSizeLevels; holeNumber += 1) {
                    GeographyPointValue offset = irCenter.add(y.mul(ymul*holeNumber));
                    GeographyPointValue center = firstCenter.add(offset);
                    GeographyPointValue firstVertex = firstFirstVertex.add(offset);
                    oneRadius.add(PolygonFactory.CreateStar(center, firstVertex, numSides, (ratioLevel + 1.0)/numIRLevels, (holeNumber+0.0)/numHoleSizeLevels));
                }
                oneSize.add(oneRadius);
            }
            answer.add(oneSize);
        }
        return answer;
    }

    public static String formatWKT(String wkt) {
        return wkt.replaceAll("([0-9]), ", "$1,\n         ").replaceAll("[)], [(]", "),\n        (");
    }

    private static int getIntArg(String args[], int arg, String msg) {
        int ans = -1;
        if (args.length <= arg) {
            System.err.printf("%s\n", msg);
            System.exit(100);
        } else {
            try {
                ans = Integer.parseInt(args[arg]);
            } catch (IllegalArgumentException ex) {
                System.err.printf("%s\n", msg);
                System.exit(100);
            }
        }
        return ans;
    }
    private static double getDoubleArg(String args[], int arg, String msg) {
        double ans = -1;
        if (args.length <= arg) {
            System.err.printf("%s\n", msg);
            System.exit(100);
        } else {
            try {
                ans = Double.parseDouble(args[arg]);
            } catch (IllegalArgumentException ex) {
                System.err.printf("%s\n", msg);
                System.exit(100);
            }
        }
        return ans;
    }

    /**
     * This main routine is useful for manual testing.  The idea is that one
     * runs this routine and WKT polygons are printed.  These can be displayed
     * with qgis or some other display tool.
     *
     * @param arg
     */
    public static void main(String args[]) {
        boolean doStars = false;
        boolean doRegs = false;
        int minVerts = 3;
        int maxVerts = 12;
        int numIRs = 5;
        int numHoles = 5;
        double xmul = 3.0;
        double ymul = 3.0;
        for (int arg = 0; arg < args.length; arg += 1) {
            if (args[arg].equals("--stars")) {
                doStars = true;
            } else if (args[arg].equals("--reg")) {
                doRegs = true;
            } else if (args[arg].equals("--minVerts")) {
                minVerts = getIntArg(args, ++arg, "--minVerts expects one integer parameters");
            } else if (args[arg].equals("--maxVerts")) {
                maxVerts = getIntArg(args, ++arg, "--maxVerts expects one integer parameters");
            } else if (args[arg].equals("--numHoles")) {
                numHoles = getIntArg(args, ++arg, "--numHoles expects one integer parameter");
            } else if (args[arg].equals("--numIRs")) {
                numIRs = getIntArg(args, ++arg, "--numIRs expects one integer parameter");
            } else if (args[arg].equals("--xmul")) {
                xmul = getDoubleArg(args, ++arg, "--xmul expects one double parameter");
            } else if (args[arg].equals("--ymul")) {
                ymul = getDoubleArg(args, ++arg, "--ymul expects one double parameter");
            } else {
                System.err.printf("Unknown command line parameter \"%s\"\n", args[arg]);
                System.exit(100);
            }
        }
        GeographyPointValue center = origin.add(x.mul(10).add(y.mul(10)));
        GeographyPointValue firstVertex = center.add(x.mul(CENTER_SHRINK*xmul).add(y.mul(CENTER_SHRINK*ymul)));
        if (doRegs) {
            List<List<GeographyValue>> polys = makeRegularConvexPolygons(center, firstVertex, minVerts, maxVerts, numHoles, xmul, ymul);
            for (int nsides = 0; nsides < polys.size(); nsides += 1) {
                for (int holeSize = 0; holeSize < 5; holeSize += 1) {
                    System.out.printf("%s\n",
                                      formatWKT(polys.get(nsides).get(holeSize).toString()));
                }
            }
        }
        if (doStars) {
            GeographyPointValue scenter = center;
            GeographyPointValue sfirstVertex = firstVertex;
            List<List<List<GeographyValue>>> stars = makeStarPolygons(scenter, sfirstVertex, minVerts, maxVerts, numIRs, numHoles, xmul, ymul);
            for (int nsides = 0; nsides < stars.size(); nsides += 1) {
                List<List<GeographyValue>> oneSize = stars.get(nsides);
                for (int innerRadiusIdx = 0; innerRadiusIdx < oneSize.size(); innerRadiusIdx += 1) {
                    List<GeographyValue> oneInnerRadius = oneSize.get(innerRadiusIdx);
                    for (int holeSizeIdx = 0; holeSizeIdx < oneInnerRadius.size(); holeSizeIdx += 1) {
                        GeographyValue oneStar = oneInnerRadius.get(holeSizeIdx);
                        System.out.printf("%s\n", formatWKT(oneStar.toString()));
                    }
                }
            }
        }
    }
}
