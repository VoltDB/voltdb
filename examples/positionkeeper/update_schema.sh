#!/usr/bin/env bash

. ./compile.sh

# live update of schema
voltadmin update ${CATALOG_NAME}.jar deployment.xml

