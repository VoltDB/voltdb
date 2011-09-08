#!/bin/bash

# run this script from the root directory of the kit to be tested
# set TOOLSDIR to a directory with eng/trunk/tools checked out into it
# set SUPDIR to a directory with the support repository root checked out into it

ROOTDIR=`pwd`
VOLTDB=${ROOTDIR}/voltdb
BUILDSTRING="Build: 1.3.6.1"


cd ${ROOTDIR}/examples/auction
${TOOLSDIR}/auction.exp "${BUILDSTRING}" || exit 1
${TOOLSDIR}/auction.sh || exit 1
cd ${SUPDIR}/customers/ms/trunk
VOLTDB=${VOLTDB} ./ms.exp "${BUILDSTRING}" || exit 1
cd ${SUPDIR}/customers/zynga/trunk
VOLTDB=${VOLTDB} ./z.exp "${BUILDSTRING}" || exit 1
cd ${ROOTDIR}/tools
${TOOLSDIR}/generate.exp "${BUILDSTRING}" || exit 1
cd ${ROOTDIR}/examples/voter
${TOOLSDIR}/voter.exp "${BUILDSTRING}" || exit 1
cd ${ROOTDIR}/examples/key_value
${TOOLSDIR}/key_value.exp "${BUILDSTRING}" || exit 1
cd ${SUPDIR}/tpcc/trunk
VOLTDB=${VOLTDB} ./tpcc.exp "${BUILDSTRING}" || exit 1

cd ${ROOTDIR}
