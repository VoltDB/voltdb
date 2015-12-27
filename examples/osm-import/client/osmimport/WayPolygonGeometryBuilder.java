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
	 * OSM stores each ring of a polygon independently, the rings of a polygon need to
	 * be combined through relationships.  This method only returns a single ring of a Polygon,
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
				Point point = new Point(longitude,latitude);
				points.add(point);
			}
		}
		return new LinearRing(points.toArray(new Point[0]));
	}

}
