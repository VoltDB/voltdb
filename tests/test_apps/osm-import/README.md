Open Streetmap Import application
==============================

This example demonstrates how to import Open Street Map (OSM) data into VoltDB
using geospatial features.  It is based on the OSM "osmosis" postGIS import application
and uses a similiar schema.
The application takes an .osm xml file as input.  It can be run multiple times to add more data.

Voltdb 6.0 doesn't support the LineString geometry type, but there is example code to handle this in the future when it's added.

