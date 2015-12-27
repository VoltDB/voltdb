Open Streetmap Import application
==============================

This example demonstrates how to import open street map (OSM) data into voltdb
using geospatial features.  It is based on the OSM "osmosis" postgis import application
and uses a similiar schema.
The application takes an .osm xml file as input.  It can be run multiple times to add more data.

Voltdb 6.0 doesn't support the LineString geometry type, but their is example code to handle this in the future when it's added.

