#!/bin/bash -e

# Directory containing trial license files
LICENSE_SOURCE_DIRECTORY="/home/test/workspace/pro-branch-rest/licensedata"

# VoltDB Version of the kit we are repacking
ACTIVE_VERSION="1.3.1"

# Create a license that lasts 45 days (30 days from 2 weeks out)
LICENSE_SOURCE=$LICENSE_SOURCE_DIRECTORY/`date --date='14 days' +"trial_%Y-%m-%d.xml"`

# Setup a clean working area
rm -rf /tmp/trialkitbuilder
mkdir -p /tmp/trialkitbuilder
cd /tmp/trialkitbuilder

# Grab the source & extract - assuming here that we grab the kit from the community site and push it right back
scp root@community.voltdb.com:/var/www/drupal/sites/default/files/archive/$ACTIVE_VERSION/voltdb-ent-$ACTIVE_VERSION.tar.gz .
tar -xzf voltdb-ent-$ACTIVE_VERSION.tar.gz
rm voltdb-ent-$ACTIVE_VERSION.tar.gz

# Copy the license file & rebuild archive
cp $LICENSE_SOURCE voltdb-ent-$ACTIVE_VERSION/management/license.xml
ls voltdb-ent-$ACTIVE_VERSION/management
tar -czf voltdb-ent-$ACTIVE_VERSION.tar.gz voltdb-ent-$ACTIVE_VERSION

# Push back to target (commented out - this is still experimental)
## scp voltdb-ent-$ACTIVE_VERSION.tar.gz root@community.voltdb.com:/var/www/drupal/sites/default/files/archive/$ACTIVE_VERSION

# Cleanup
#rm -rf /tmp/trialkitbuilder/
