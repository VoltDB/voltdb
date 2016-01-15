Open Streetmap Import application
==============================

This example demonstrates how to import Open Street Map (OSM) data into VoltDB
using geospatial features.  It is based on the OSM "osmosis" postGIS import application
and uses a similiar schema.

It uses a single local server and can be run as follows to start the server and run the demo.
osm-import> ./run.sh

If run standalone, it can take an Open Street Map .osm xml file and import the raw data as follows:
java ... osmimport.OSMImport --file=monaco.osm --server=localhost

Where monaco.osm is a small subset of OSM data pulled from the geofabrik.de site ( http://download.geofabrik.de/europe/monaco.html)
but any .osm file can be imported.  Each subsequent run will append more data.

This application only import's polygon data because VoltDB 6.0 doesn't support the LineString geometry type, but there is example code
to handle this in the future when it's added.

Be aware the OSM data is crowd sourced and may not adhere to most of the isValid() rules we have for polygons, my initial tests showed
that only 1/2 of the polygons where valid.  We can still store those invalid polygons, and use them in outside of volt, but you
wouldn't get valid results if you use any of the built in functions, such as CENTROID or CONTAINS, on these invalid polygons.


