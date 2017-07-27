# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
########################################################################
#
# A CMake build configuration for the VoltDB Execution Engine.
# This is called from the CMakeLists.txt file in the root of
# the VoltDB source tree.  So you probably don't want to be
# using this by itself.
#
# The two artifacts are:
#   1.) The jni library, which will be in
#           ${CMAKE_BINARY_}/nativelibs/libvoltdb-VERSION.so
#       for Linux and
#           ${CMAKE_BINARY_DIR}/nativelibs/libvoltdb-VERSION.dylib
#       for Darwin.
#   2.) The voltdbipcrun program, which is used for memory leak detection,
#       and to debug this EE.  This will be found in
#           ${CMAKE_BINARY_DIR}/prod/voltdbipcrun
#       on both Linux and Darwin.
#
# Note that we don't build any tests here.  The tests are built
# in tests/ee, which has its own CMakeLists.txt file and is
# built separately from the Execution Engine.
#
########################################################################
CMAKE_MINIMUM_REQUIRED (VERSION 2.8.11 FATAL_ERROR)
PROJECT(VoltDB_EE)

BANNER("Configuring the VoltDB Execution Engine Runtime."
       "CMAKE_SOURCE_DIRECTORY: ${CMAKE_SOURCE_DIRECTORY}"
       "CMAKE_BINARY_DIRECTORY: ${CMAKE_BINARY_DIRECTORY}"
       "${PROJECT_NAME}: PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}"
       "${PROJECT_NAME}: PROJECT_BINARY_DIRECTORY is ${${PROJECT_NAME}_BINARY_DIR}"
       "VOLTDB_BUILD_TYPE: ${VOLTDB_BUILD_TYPE}"
   )

IF ( (CMAKE_SYSTEM_NAME STREQUAL "Darwin") AND ( (CMAKE_VERSION VERSION_GREATER 3.0.0) OR (CMAKE_VERSION VERSION_EQUAL 3.0.0) ) )
    # Newer versions of CMake than we use set this
    # policy on the mac.  There there is a way to set the
    # rpath which is different from Linux.  We just ignore
    # it, but we need to use the old behavior on the mac.
    CMAKE_POLICY(SET CMP0042 OLD)
ENDIF()

########################################################################
#
# This CMakeLists.txt file describes the builds for the VoltDB Runtime.
#
# There are a few rules which may make things be more organized.
# 1. All variables defined here should start with VOLTDB_, with a few
#    exceptions for historical reasons.  These exceptions are:
#          VOLT_LOG_LEVEL   The numeric log level.  See the values
#                           for VOLT_LEVEL_* in src/ee/common/debuglog.h
#                           The default is 500.  This is cached, so it
#                           needs to be specified to cmake each time it needs
#                           to be changed.
# 2. Object files generally go into these folders.  The variables are
#    set in the root CMakeLists.txt file.
#      ${CMAKE_BINARY_DIR}
#                           Root of the output tree.  This is
#                           generally obj/debug, obj/release or obj/memcheck.
#                           It will have -profile or -coverage at the
#                           end if those options are enabled, and this is
#                           called from the ant build process.
#       ${CMAKE_BINARY_DIR}/3pty_install
#                           All third party artifacts on which which
#                           voltdb depends go here, in lib or include.
#                           There are some other subfolders created
#                           by the third party install procedures,
#                           but we don't care about anything but the
#                           static libraries and includes.
########################################################################
#
# Options.
# These can be turned on and off to steer the build.
# Note: These don't work.  When we enable coverage
#       and profiling this is how they will be enabled, though.
#
########################################################################
OPTION(VOLTDB_USE_COVERAGE "Build for coverage analysis" OFF)
OPTION(VOLTDB_USE_PROFILING "Build for profiling analysis" OFF)

########################################################################
#
# Go and build the pcre2 library.
# We have to untar it and configure it.  We will
# do this at build time.
#
########################################################################
SET (VOLTDB_PCRE2_PREFIX ${CMAKE_BINARY_DIR}/3pty-obj/pcre2)
SET (VOLTDB_PCRE2_VERSION 10.10)
SET (VOLTDB_PCRE2_NAME pcre2-${VOLTDB_PCRE2_VERSION})
SET (VOLTDB_PCRE2_TARBALL ${CMAKE_SOURCE_DIR}/third_party/cpp/tarballs/${VOLTDB_PCRE2_NAME}.tgz)
SET (VOLTDB_PCRE2_SRC ${VOLTDB_PCRE2_PREFIX}/src/${VOLTDB_PCRE2_NAME})
SET (VOLTDB_PCRE2_OBJ ${VOLTDB_PCRE2_PREFIX}/obj/${VOLTDB_PCRE2_NAME})

BANNER("Describing PCRE2"
       "  PREFIX at ${VOLTDB_PCRE2_PREFIX}"
       "  SOURCE at ${VOLTDB_PCRE2_SRC}"
       "  BINARY at ${VOLTDB_PCRE2_OBJ}"
       "  TARBALL at ${VOLTDB_PCRE2_TARBALL}"
       "  CONFIGURE_COMMAND: ${VOLTDB_PCRE2_SRC}/configure -disable-shared --with-pic --prefix=${CMAKE_BINARY_DIR}/3pty-install"
   )
ExternalProject_Add(pcre2
  PREFIX ${VOLTDB_PCRE2_PREFIX}
  URL ${VOLTDB_PCRE2_TARBALL}
  SOURCE_DIR ${VOLTDB_PCRE2_SRC}
  BINARY_DIR ${VOLTDB_PCRE2_OBJ}
  CONFIGURE_COMMAND ${VOLTDB_PCRE2_SRC}/configure -disable-shared --with-pic --prefix=${CMAKE_BINARY_DIR}/3pty-install
  BUILD_COMMAND make
  INSTALL_COMMAND make install)

########################################################################
#
# Go and build the openssl library.  We only use this
# for the arbitrary precision arithmetic code, which
# S2GEO uses.
#
########################################################################
SET (VOLTDB_OPENSSL_VERSION 1.0.2d)
SET (VOLTDB_OPENSSL_NAME openssl-${VOLTDB_OPENSSL_VERSION})
SET (VOLTDB_OPENSSL_TARBALL ${CMAKE_SOURCE_DIR}/third_party/cpp/tarballs/${VOLTDB_OPENSSL_NAME}.tgz)
SET (VOLTDB_OPENSSL_PREFIX ${CMAKE_BINARY_DIR}/3pty-obj/openssl)
# Binary and source are the same for openssl.  There
# are not obvious ways to do out-of-tree builds.
SET (VOLTDB_OPENSSL_SRC ${VOLTDB_OPENSSL_PREFIX}/src/${VOLTDB_OPENSSL_NAME})
SET (VOLTDB_OPENSSL_OBJ ${VOLTDB_OPENSSL_PREFIX}/src/${VOLTDB_OPENSSL_NAME})

BANNER("Describing CRYPTO"
       "  PREFIX at ${VOLTDB_OPENSSL_PREFIX}"
       "  SOURCE at ${VOLTDB_OPENSSL_SRC}"
       "  BINARY at ${VOLTDB_OPENSSL_OBJ}"
       "  TARBALL at ${VOLTDB_OPENSSL_TARBALL}"
       "  CONFIGURE_COMMAND: ./Configure --prefix=${CMAKE_BINARY_DIR}/3pty-install ${VOLTDB_OPENSSL_TOKEN}"
   )
ExternalProject_Add(crypto
  PREFIX ${VOLTDB_OPENSSL_PREFIX}
  URL ${VOLTDB_OPENSSL_TARBALL}
  SOURCE_DIR ${VOLTDB_OPENSSL_SRC}
  BINARY_DIR ${VOLTDB_OPENSSL_OBJ}
  CONFIGURE_COMMAND ./Configure --prefix=${CMAKE_BINARY_DIR}/3pty-install ${VOLTDB_OPENSSL_TOKEN}
  BUILD_COMMAND make
  INSTALL_COMMAND make install)

########################################################################
#
# Go and build the S2GEO library.
#
########################################################################
SET (VOLTDB_S2GEO_SRC "${CMAKE_SOURCE_DIR}/third_party/cpp/google-s2-geometry")
SET (VOLTDB_S2GEO_OBJ ${CMAKE_BINARY_DIR}/3pty-obj/google-s2-geometry)
SET (VOLTDB_S2GEO_CMAKE_CONFIG
  -DCXX_VERSION_FLAG=${CXX_VERSION_FLAG}
  -DVOLTDB_THIRD_PARTY_CPP_DIR=${CMAKE_SOURCE_DIR}/third_party/cpp
  -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/3pty-install
  -DCMAKE_BUILD_TYPE=${VOLTDB_BUILD_TYPE})
SET (VOLTDB_S2GEO_PREFIX ${CMAKE_BINARY_DIR}/3pty-obj)

BANNER("Describing S2GEO"
       "  PREFIX at ${VOLTDB_S2GEO_PREFIX}"
       "  SOURCE at ${VOLTDB_S2GEO_SRC}"
       "  BINARY at ${VOLTDB_S2GEO_OBJ}"
       "  CMAKE_ARGS: ${VOLTDB_S2GEO_CMAKE_CONFIG}"
       "  CMAKE_GENERATOR: ${CMAKE_GENERATOR}"
   )
ExternalProject_Add(s2geo
  DEPENDS crypto
  PREFIX ${VOLTDB_S2GEO_PREFIX}
  SOURCE_DIR ${VOLTDB_S2GEO_SRC}
  CMAKE_ARGS ${VOLTDB_S2GEO_CMAKE_CONFIG}
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  BINARY_DIR ${VOLTDB_S2GEO_OBJ})

########################################################################
#
# Go and build the VoltDB library and IPC command.
#
########################################################################
SET (VOLTDB_SRC
  catalog/catalog.cpp
  catalog/catalogtype.cpp
  catalog/cluster.cpp
  catalog/column.cpp
  catalog/columnref.cpp
  catalog/connector.cpp
  catalog/connectortableinfo.cpp
  catalog/connectorproperty.cpp
  catalog/constraint.cpp
  catalog/constraintref.cpp
  catalog/database.cpp
  catalog/index.cpp
  catalog/indexref.cpp
  catalog/materializedviewhandlerinfo.cpp
  catalog/materializedviewinfo.cpp
  catalog/planfragment.cpp
  catalog/statement.cpp
  catalog/table.cpp
  catalog/tableref.cpp
  structures/ContiguousAllocator.cpp
  common/FatalException.cpp
  common/ThreadLocalPool.cpp
  common/SegvException.cpp
  common/SerializableEEException.cpp
  common/SQLException.cpp
  common/InterruptException.cpp
  common/StringRef.cpp
  common/tabletuple.cpp
  common/TupleSchema.cpp
  common/types.cpp
  common/UndoLog.cpp
  common/LargeTempTableBlockCache.cpp
  common/NValue.cpp
  common/RecoveryProtoMessage.cpp
  common/RecoveryProtoMessageBuilder.cpp
  common/executorcontext.cpp
  common/serializeio.cpp
  common/StreamPredicateList.cpp
  common/Topend.cpp
  common/TupleOutputStream.cpp
  common/TupleOutputStreamProcessor.cpp
  common/MiscUtil.cpp
  common/debuglog.cpp
  execution/FragmentManager.cpp
  execution/JNITopend.cpp
  execution/VoltDBEngine.cpp
  execution/ExecutorVector.cpp
  executors/OptimizedProjector.cpp
  executors/abstractexecutor.cpp
  executors/abstractjoinexecutor.cpp
  executors/aggregateexecutor.cpp
  executors/deleteexecutor.cpp
  executors/executorfactory.cpp
  executors/executorutil.cpp
  executors/indexcountexecutor.cpp
  executors/indexscanexecutor.cpp
  executors/insertexecutor.cpp
  executors/limitexecutor.cpp
  executors/materializedscanexecutor.cpp
  executors/materializeexecutor.cpp
  executors/mergereceiveexecutor.cpp
  executors/nestloopexecutor.cpp
  executors/nestloopindexexecutor.cpp
  executors/orderbyexecutor.cpp
  executors/windowfunctionexecutor.cpp
  executors/projectionexecutor.cpp
  executors/receiveexecutor.cpp
  executors/sendexecutor.cpp
  executors/seqscanexecutor.cpp
  executors/swaptablesexecutor.cpp
  executors/tablecountexecutor.cpp
  executors/tuplescanexecutor.cpp
  executors/unionexecutor.cpp
  executors/updateexecutor.cpp
  expressions/abstractexpression.cpp
  expressions/expressionutil.cpp
  expressions/functionexpression.cpp
  expressions/geofunctions.cpp
  expressions/operatorexpression.cpp
  expressions/parametervalueexpression.cpp
  expressions/scalarvalueexpression.cpp
  expressions/subqueryexpression.cpp
  expressions/tupleaddressexpression.cpp
  expressions/vectorexpression.cpp
  plannodes/abstractjoinnode.cpp
  plannodes/abstractoperationnode.cpp
  plannodes/abstractplannode.cpp
  plannodes/abstractreceivenode.cpp
  plannodes/abstractscannode.cpp
  plannodes/aggregatenode.cpp
  plannodes/deletenode.cpp
  plannodes/indexscannode.cpp
  plannodes/indexcountnode.cpp
  plannodes/tablecountnode.cpp
  plannodes/insertnode.cpp
  plannodes/limitnode.cpp
  plannodes/materializenode.cpp
  plannodes/materializedscanplannode.cpp
  plannodes/mergereceivenode.cpp
  plannodes/nestloopindexnode.cpp
  plannodes/nestloopnode.cpp
  plannodes/orderbynode.cpp
  plannodes/plannodefragment.cpp
  plannodes/plannodeutil.cpp
  plannodes/windowfunctionnode.cpp
  plannodes/projectionnode.cpp
  plannodes/receivenode.cpp
  plannodes/SchemaColumn.cpp
  plannodes/sendnode.cpp
  plannodes/seqscannode.cpp
  plannodes/swaptablesnode.cpp
  plannodes/tuplescannode.cpp
  plannodes/unionnode.cpp
  plannodes/updatenode.cpp
  indexes/CoveringCellIndex.cpp
  indexes/IndexStats.cpp
  indexes/tableindex.cpp
  indexes/tableindexfactory.cpp
  storage/AbstractDRTupleStream.cpp
  storage/BinaryLogSink.cpp
  storage/BinaryLogSinkWrapper.cpp
  storage/ConstraintFailureException.cpp
  storage/constraintutil.cpp
  storage/CopyOnWriteContext.cpp
  storage/CopyOnWriteIterator.cpp
  storage/DRTupleStream.cpp
  storage/ElasticContext.cpp
  storage/ElasticIndex.cpp
  storage/ElasticIndexReadContext.cpp
  storage/ElasticScanner.cpp
  storage/ExportTupleStream.cpp
  storage/LargeTempTable.cpp
  storage/LargeTempTableBlock.cpp
  storage/MaterializedViewHandler.cpp
  storage/MaterializedViewTriggerForInsert.cpp
  storage/MaterializedViewTriggerForWrite.cpp
  storage/persistenttable.cpp
  storage/PersistentTableStats.cpp
  storage/RecoveryContext.cpp
  storage/streamedtable.cpp
  storage/StreamedTableStats.cpp
  storage/table.cpp
  storage/TableCatalogDelegate.cpp
  storage/tablefactory.cpp
  storage/TableStats.cpp
  storage/TableStreamer.cpp
  storage/TableStreamerContext.cpp
  storage/tableutil.cpp
  storage/tabletuplefilter.cpp
  storage/temptable.cpp
  storage/TempTableLimits.cpp
  storage/TupleBlock.cpp
  storage/TupleStreamBase.cpp
  stats/StatsAgent.cpp
  stats/StatsSource.cpp
  logging/JNILogProxy.cpp
  logging/LogManager.cpp
)

SET (VOLTDB_THIRD_PARTY_SRC
  ${CMAKE_SOURCE_DIR}/third_party/cpp/jsoncpp/jsoncpp.cpp
  ${CMAKE_SOURCE_DIR}/third_party/cpp/crc/crc32c.cc
  ${CMAKE_SOURCE_DIR}/third_party/cpp/crc/crc32ctables.cc
  ${CMAKE_SOURCE_DIR}/third_party/cpp/murmur3/MurmurHash3.cpp
  ${CMAKE_SOURCE_DIR}/third_party/cpp/sha1/sha1.cpp
)

SET (VOLTDB_JNI_SRC
  voltdbjni.cpp
  )
SET (VOLTDB_IPC_SRC
  voltdbipc.cpp
)
ADD_LIBRARY(voltdbobjs OBJECT ${VOLTDB_SRC})
ADD_LIBRARY(voltdbjniobjs OBJECT ${VOLTDB_JNI_SRC})
ADD_LIBRARY(voltdbipcobjs OBJECT ${VOLTDB_IPC_SRC})
ADD_LIBRARY(third_party_objs OBJECT ${VOLTDB_THIRD_PARTY_SRC})

ADD_DEPENDENCIES(voltdbobjs s2geo crypto pcre2)
ADD_DEPENDENCIES(voltdbipcobjs s2geo crypto pcre2)
ADD_DEPENDENCIES(voltdbjniobjs s2geo crypto pcre2)

TARGET_INCLUDE_DIRECTORIES(voltdbobjs
  SYSTEM PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdbobjs
  PUBLIC
  ${${PROJECT_NAME}_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)
TARGET_INCLUDE_DIRECTORIES(voltdbipcobjs
  SYSTEM PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdbipcobjs
  PUBLIC
  ${${PROJECT_NAME}_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)
TARGET_INCLUDE_DIRECTORIES(voltdbjniobjs
  SYSTEM PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdbjniobjs
  PUBLIC
  ${${PROJECT_NAME}_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)
TARGET_INCLUDE_DIRECTORIES(third_party_objs
  SYSTEM PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp)

########################################################################
#
# Adding the voltdb jni library.
#
########################################################################
SET (VOLTDB_LIBNAME voltdb-${VOLTDB_VERSION})
ADD_LIBRARY(${VOLTDB_LIBNAME}
  SHARED
  $<TARGET_OBJECTS:voltdbobjs>
  $<TARGET_OBJECTS:voltdbjniobjs>
  $<TARGET_OBJECTS:third_party_objs>
  )

TARGET_LINK_LIBRARIES(${VOLTDB_LIBNAME}
  -L${CMAKE_BINARY_DIR}/3pty-install/lib
  -lpcre2-8 -ls2geo -lcrypto
  ${VOLTDB_LINK_FLAGS})

########################################################################
#
# Adding the voltdbipc executable.
#
########################################################################
SET (VOLTDB_IPCRUN_NAME voltdbipc)
ADD_EXECUTABLE(${VOLTDB_IPCRUN_NAME}
  $<TARGET_OBJECTS:voltdbobjs>
  $<TARGET_OBJECTS:third_party_objs>
  $<TARGET_OBJECTS:voltdbipcobjs>
)

TARGET_LINK_LIBRARIES(${VOLTDB_IPCRUN_NAME}
  ${VOLTDB_LINK_FLAGS}
  -L${CMAKE_BINARY_DIR}/3pty-install/lib
  -lpcre2-8 -ls2geo -lcrypto
  -ldl)

########################################################################
#
# Where to install things, and what to install.
#
########################################################################
INSTALL(TARGETS ${VOLTDB_LIBNAME}
  DESTINATION ${CMAKE_BINARY_DIR}/nativelibs)

INSTALL(TARGETS ${VOLTDB_IPCRUN_NAME}
  DESTINATION ${CMAKE_BINARY_DIR}/prod)

########################################################################
#
# What about voltrun?  Do we use that anymore?  Where is it used?
#
########################################################################
#
# Add a custom target to build the two build artifacts we care about.
#
########################################################################
ADD_CUSTOM_TARGET(build
  DEPENDS
  ${VOLTDB_LIBNAME}
  ${VOLTDB_IPCRUN_NAME})
