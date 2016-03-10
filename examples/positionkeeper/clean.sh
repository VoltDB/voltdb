#!/usr/bin/env bash

# clean up temp files & folders in this project


# db
cd db
rm -rf obj log debugoutput statement-plans voltdbroot *.jar catalog-report.html
cd ..

# client
cd client
rm -rf obj log loader_logs
cd ..

echo "removed temp files & folders"
