/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package genqa.procedures;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.PolygonFactory;

public class SampleGeoRecord extends SampleRecord {

    public final Object type_null_geography;
    public final Object type_not_null_geography;
    public final Object type_null_geography_point;
    public final Object type_not_null_geography_point;

    public SampleGeoRecord(long rowid, Random rand) {
       super(rowid,rand);
       this.type_null_geography      = nextGeography(rand, true, 128, 1024);
       this.type_not_null_geography  = nextGeography(rand, 128, 1024);
       this.type_null_geography_point = nextGeographyPoint(rand,true,128,1024);
       this.type_not_null_geography_point = nextGeographyPoint(rand,128,1024);
    }

    private static Object nextGeography(Random rand, int minLength, int maxLength)
    {
        return nextGeography(rand, false, minLength, maxLength);
    }

    // POLYGON ( ( 1.5 3.0, 0.0 0.0, 3.0 0.0, 1.5 3.0 ) )
    private static Object nextGeography(Random rand, boolean isNullable, int minLength, int maxLength)
    {
        if (isNullable && rand.nextBoolean()) return null;
        // we need to have at least 4 vertices
        int numVertices = rand.nextInt(6)+4;
        double sizeOfHole = rand.nextDouble();
        GeographyPointValue center = GeographyPointValue.fromWKT("POINT(0 0)");
        GeographyPointValue firstVertex = GeographyPointValue.fromWKT("POINT(1 1)");
        GeographyValue poly = PolygonFactory.CreateRegularConvex(center, firstVertex, numVertices, sizeOfHole);
        return poly;
    }

    private static Object nextGeographyPoint(Random rand, int minLength, int maxLength)
    {
        return nextGeographyPoint(rand, false, minLength, maxLength);
    }

    // Geography points have a syntax like this:
    // POINT(-74.0059 40.7127)
    private static Object nextGeographyPoint(Random rand, boolean isNullable, int minLength, int maxLength)
    {
        if (isNullable && rand.nextBoolean()) return null;
        int pointX = rand.nextInt(90);
        int pointY = rand.nextInt(90);

        String wktPoint = "POINT("+String.valueOf(pointX)+ " "+String.valueOf(pointY)+")";
        return GeographyPointValue.fromWKT(wktPoint);
    }
}
