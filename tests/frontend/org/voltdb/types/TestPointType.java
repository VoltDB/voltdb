package org.voltdb.types;

import junit.framework.TestCase;

public class TestPointType extends TestCase {

    public void testBasic() {
        PointType point = new PointType();
        assertTrue(point.isNull());
        assertEquals(0.0f, point.getLatitude(), 0.000001);
        assertEquals(0.0f, point.getLongitude(), 0.000001);

    }
}
