#!/bin/bash -e

# Parameters to pass/auto-discover
LICENSE_SOURCE_DIRECTORY="/home/sebc/svndev/pro.trunk/licensedata"
ACTIVE_VERSION="1.2.1.06"

# Working variables
LICENSE_SOURCE=$LICENSE_SOURCE_DIRECTORY/`date +"trial_%Y-%m-%d.xml"`

# Setup a clean working area
rm -rf /tmp/trialkitbuilder
mkdir -p /tmp/trialkitbuilder
cd /tmp/trialkitbuilder

# Grab the source & extract - assuming here that we grab the kit from the community site and push it right back
scp root@community.voltdb.com:/var/www/drupal/sites/default/files/archive/$ACTIVE_VERSION/voltdb-ent-$ACTIVE_VERSION.tar.gz .
tar -xzf voltdb-ent-$ACTIVE_VERSION.tar.gz
rm voltdb-ent-$ACTIVE_VERSION.tar.gz

# Copy the license file & rebuild archive
cp $LICENSE_SOURCE_DIRECTORY/`date +"trial_%Y-%m-%d.xml"` voltdb-ent-$ACTIVE_VERSION/management/license.xml
ls voltdb-ent-$ACTIVE_VERSION/management
tar -czf voltdb-ent-$ACTIVE_VERSION.tar.gz voltdb-ent-$ACTIVE_VERSION

# Push back to target (commented out - this is still experimental)
## scp voltdb-ent-$ACTIVE_VERSION.tar.gz root@community.voltdb.com:/var/www/drupal/sites/default/files/archive/$ACTIVE_VERSION

# Cleanup
rm -rf /tmp/trialkitbuilder/

