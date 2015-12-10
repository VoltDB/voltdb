#!/bin/bash

voltdb create &

sleep 5

cat ddl.sql | sqlcmd

cat us_cities.csv | csvloader --quotechar \' us_cities
cat us_states.csv | csvloader --quotechar \' us_states
cat us_counties.csv | csvloader --quotechar \' us_counties
