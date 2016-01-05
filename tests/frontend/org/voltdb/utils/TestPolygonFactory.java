/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
            List<List<GeographyPointValue>> loops = regularConvex.getLoops();
            assertEquals(1, loops.size());
            List<GeographyPointValue> loop = loops.get(0);
            assertEquals(npts + 1, loop.size());
            regularConvex = PolygonFactory.CreateRegularConvex(origin,  startVertex, npts, 0.5);
            loops = regularConvex.getLoops();
            assertEquals(2, loops.size());
            assertEquals(npts + 1, loops.get(0).size());
            assertEquals(npts + 1, loops.get(1).size());
        }
    }

    public void testStarPolygon() throws Exception {
        for (int idx = 3; idx < 10; idx += 1) {
            GeographyValue star = PolygonFactory.CreateStar(origin, y.mul(20.0), idx, 0.5, 0.0);
            List<List<GeographyPointValue>> loops = star.getLoops();
            assertEquals(1, loops.size());
            List<GeographyPointValue> shell = loops.get(0);
            assertEquals(2*idx+1, shell.size());
            star = PolygonFactory.CreateStar(origin, y.mul(20).add(x.mul(20)), idx, 0.5, 0.1);
            loops = star.getLoops();
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
     * equal to k*0.2.  If k == 0 there is no hole.  Note that k ranges between
     * 0 and 4, so k*0.2 ranges between 0 and 0.8.
     *
     * @return
     */
    private static List<List<GeographyValue>> makeRegularConvexPolygons(GeographyPointValue firstCenter,
                                                                        GeographyPointValue firstFirstVertex,
                                                                        double xmul,
                                                                        double ymul) {
        List<List<GeographyValue>> answer = new ArrayList<List<GeographyValue>>();
        for (int idx = 0; idx < 10; idx += 1) {
            List<GeographyValue> oneSize = new ArrayList<GeographyValue>();
            for (int hidx = 0; hidx < 5; hidx += 1) {
                GeographyPointValue center = firstCenter.add(x.mul(xmul*idx).add(y.mul(ymul*hidx)));
                GeographyPointValue firstVertex = firstFirstVertex.add(x.mul(xmul*idx).add(y.mul(ymul*hidx)));
                oneSize.add(PolygonFactory.CreateRegularConvex(center, firstVertex, idx + 3, hidx*0.2));
            }
            answer.add(oneSize);
        }
        return answer;
    }

    /**
     * Create many star-like polygons.  In returnValue.get(n-minNumPoints).get(s).get(k) we put an
     * n-pointed polygon with the given center and start vertex.  The inner radius is
     * (numIRLevels-1-s)/numIRLevels.  The hole size is k/numHoleSizeLevels.
     * If k == 0, there is no hole.
     *
     * Note that n ranges between minNumPoints and maxNumPoints, s between 0 and numIRLevels-1
     * and k between 0 and numHoleSizeLevels-1.  So, the hole size and inner radius must
     * both be less than 1, and the inner radius must be greater than zero.  The hole size
     * can be zero.
     *
     * @return
     */
    private static List<List<List<GeographyValue>>> makeStarPolygons(GeographyPointValue firstCenter,
                                                                     GeographyPointValue firstFirstVertex,
                                                                     int minNumPoints,
                                                                     int maxNumPoints,
                                                                     int numIRLevels,
                                                                     int numHoleSizeLevels,
                                                                     double xmul,
                                                                     double ymul) {
        List<List<List<GeographyValue>>> answer = new ArrayList<List<List<GeographyValue>>>();
        int npoints = maxNumPoints - minNumPoints + 1;
        for (int idx = 0; idx < npoints; idx += 1) {
            GeographyPointValue column = x.mul(xmul*idx);
            List<List<GeographyValue>> oneSize = new ArrayList<List<GeographyValue>>();
            for (int irdx = 0; irdx < numIRLevels; irdx += 1) {
                GeographyPointValue irCenter = y.mul(numIRLevels*ymul*irdx);
                List<GeographyValue> oneRadius = new ArrayList<GeographyValue>();
                for (int hidx = 0; hidx < numHoleSizeLevels; hidx += 1) {
                    GeographyPointValue hCenter = irCenter.add(y.mul(ymul*hidx));
                    GeographyPointValue center = firstCenter.add(column.add(hCenter));
                    GeographyPointValue firstVertex = firstFirstVertex.add(center);
                    oneRadius.add(PolygonFactory.CreateStar(center, firstVertex, idx + minNumPoints, (irdx + 1)*0.2, hidx*0.2));
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

    /**
     * This main routine is useful for manual testing.  The idea is that one
     * runs this routine and WKT polygons are printed.  These can be displayed
     * with qgis.
     *
     * It's unfortunate that there are no command line parameters to govern how
     * this is to be run.
     *
     * @param arg
     */
    public static void main(String arg[]) {
        GeographyPointValue center = origin;
        GeographyPointValue firstVertex = x.mul(0.1).add(y.mul(0.1));
        List<List<GeographyValue>> polys = makeRegularConvexPolygons(center, firstVertex, 1.0, 1.0);
        System.out.printf(":-------------------------------:\n");
        System.out.printf(":------- Regular Convex --------:\n");
        System.out.printf(":-------------------------------:\n");
        for (int nsides = 0; nsides < polys.size(); nsides += 1) {
            for (int holeSize = 0; holeSize < 5; holeSize += 1) {
                System.out.printf("%s\n",
                                  formatWKT(polys.get(nsides).get(holeSize).toString()));
            }
        }
        System.out.printf(":-------------------------------:\n");
        System.out.printf(":----------- Stars -------------:\n");
        System.out.printf(":-------------------------------:\n");
        List<List<List<GeographyValue>>> stars = makeStarPolygons(center, firstVertex, 11, 11, 1, 5, 1.0, 1.0);
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
