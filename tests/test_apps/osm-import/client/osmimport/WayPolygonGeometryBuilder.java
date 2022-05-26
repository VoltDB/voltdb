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
package osmimport;

import java.util.ArrayList;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.pgsimple.common.NodeLocation;
import org.openstreetmap.osmosis.pgsimple.common.NodeLocationStoreType;
import org.openstreetmap.osmosis.pgsimple.v0_6.impl.WayGeometryBuilder;
import org.postgis.LinearRing;
import org.postgis.Point;
import org.postgis.Polygon;

public class WayPolygonGeometryBuilder extends WayGeometryBuilder {

    public WayPolygonGeometryBuilder(NodeLocationStoreType storeType) {
        super(storeType);

    }

    /**
     * OSM stores each ring of a polygon independently, the rings of a polygon
     * need to be combined through relationships. This method only returns a
     * single ring of a Polygon,
     *
     * @param way
     * @return
     */
    public Polygon createPolygon(Way way) {
        LinearRing[] rings = new LinearRing[1];
        rings[0] = createRing(way);

        Polygon pg = new Polygon(rings);

        return pg;
    }

    public LinearRing createRing(Way way) {
        List<Point> points = new ArrayList<Point>();

        for (WayNode wayNode : way.getWayNodes()) {
            NodeLocation nodeLocation;
            double longitude;
            double latitude;

            nodeLocation = locationStore.getNodeLocation(wayNode.getNodeId());
            longitude = nodeLocation.getLongitude();
            latitude = nodeLocation.getLatitude();

            if (nodeLocation.isValid()) {
                Point point = new Point(longitude, latitude);
                points.add(point);
            }
        }
        return new LinearRing(points.toArray(new Point[0]));
    }

}
