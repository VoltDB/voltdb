/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.types;

import java.util.regex.Pattern;

import junit.framework.TestCase;

public class TestPointType extends TestCase {

    public void testBasic() {
        PointType point = new PointType();
        assertTrue(point.isNull());

        PointType otherPoint = new PointType();
        assertTrue(point.equals(otherPoint));

        assertFalse(point.equals(6));
    }

    public void testPointCtor() {
        PointType point = new PointType(10.333f, 20.666f);
        assertEquals(10.333f, point.getLatitude(), 0.001);
        assertEquals(20.666f, point.getLongitude(), 0.001);

        assertTrue(point.equals(point));
        assertFalse(point.isNull());

        assertEquals("POINT (10.333 20.666)", point.toString());
    }
    
    private void testOnePointFromFactory(String aWKT, float aLatitude, float aLongitude, float aEpsilon, String aErrMsg) {
    	try {
    		PointType point = PointType.pointFromText(aWKT);
    		assertEquals(aLatitude, point.getLatitude(), aEpsilon);
    		if (aErrMsg != null) {
    			assertTrue(String.format("Expected error message matching \"%s\", but got no error.", aErrMsg), aErrMsg == null);
    		}
    	} catch (Exception ex) {
    		if (aErrMsg != null) {
	    		assertTrue(String.format("Expected error message matching \"%s\", but got \"%s\"",
	    								 aErrMsg, ex.getMessage()),
	    				   Pattern.matches(aErrMsg, ex.getMessage()));
    		} else {
    			assertTrue(String.format("Unexpected error message: \"%s\"", ex.getMessage()), false);
    		}
    	}
    }
    
    public void testPointFactory() {
    	testOnePointFromFactory("point(10.333 20.666)",               10.333f,  20.666f, 0.001f, null);
    	testOnePointFromFactory("  point  (10.333   20.666)    ",     10.333f,  20.666f, 0.001f, null);
    	testOnePointFromFactory("point(-10.333 -20.666)",            -10.333f, -20.666f, 0.001f, null);
    	testOnePointFromFactory("  point  (-10.333   -20.666)    ",  -10.333f, -20.666f, 0.001f, null);
    	testOnePointFromFactory("point(10 10)",                       10.0f,    10.0f,   0.001f, null);
    	// Test latitude/longitude ranges.
    	testOnePointFromFactory("point( 100.0   100.0)",             100.0f,   100.0f,   0.001f, "Latitude \"100.0+\" out of bounds.");
    	testOnePointFromFactory("point(  45.0   360.0)",              45.0f,   360.0f,   0.001f, "Longitude \"360.0+\" out of bounds.");
    	testOnePointFromFactory("point(  45.0   270.0)",              45.0f,   360.0f,   0.001f, "Longitude \"270.0+\" out of bounds.");
    	testOnePointFromFactory("point(-100.0  -100.0)",            -100.0f,  -100.0f,   0.001f, "Latitude \"-100.0+\" out of bounds.");
    	testOnePointFromFactory("point( -45.0  -360.0)",             -45.0f,  -360.0f,   0.001f, "Longitude \"-360.0+\" out of bounds.");
    	testOnePointFromFactory("point( -45.0  -270.0)",             -45.0f,  -360.0f,   0.001f, "Longitude \"-270.0+\" out of bounds.");
    	// Syntax errors
    	//   Comma separating the coordinates.
    	testOnePointFromFactory("point(0.0, 0.0)",                     0.0f,     0.0f,   0.001f, "Cannot construct PointType value from \"point\\(0[.]0, 0[.]0\\)\"");
    }
}
