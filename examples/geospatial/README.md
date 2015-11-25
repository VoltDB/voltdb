Geospatial ad brokering application
==============================

This example demonstrates geospatial queries which can be performed in
VoltDB.  It simulates an ad broker app where there are several
advertisers who have ads they would like to serve to users of mobile
devices.  Advertisers periodically make bids of how much they will pay
to have an ad served to a user for a specified duration and within a
particular region.

The demo simulates users opening up ad-serving apps on their mobile
devices which then request an ad based on the device's location and
the current time.  This operation will require executing a geospatial
query to see if the location of the device is contained by any of the
regions for which a bid is active.  Many thousands of such requests
can be performed per second.
