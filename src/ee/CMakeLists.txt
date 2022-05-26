# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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
#
# A Note On Names.
#
# This project's name is VoltDB_EE.  This is in the variable
# PROJECT_NAME.  The source and binary directories for this project
# are in ${${PROJECT_NAME}_SOURCE_DIR} and ${${PROJECT_NAME}_BINARY_DIR}.
# the name ${CMAKE_SOURCE_DIR} and ${CMAKE_BINARY_DIR} are the names of
# the source and binary directories of the parent project.  These are
# the root of the voltdb source tree and the value of
# ${CMAKE_SOURCE_DIR}/obj/${VOLTDB_BUILD_TYPE} respectively.
#
########################################################################
CMAKE_MINIMUM_REQUIRED (VERSION 2.8.11 FATAL_ERROR)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

PROJECT(VoltDB_EE)

include(${CMAKE_SOURCE_DIR}/tools/VoltDB.cmake)
include(${CMAKE_SOURCE_DIR}/tools/VoltDBCompilation.cmake)

BANNER("Configuring the VoltDB Execution Engine Runtime."
       "CMAKE_SOURCE_DIRECTORY: ${CMAKE_SOURCE_DIR}"
       "CMAKE_BINARY_DIRECTORY: ${CMAKE_BINARY_DIR}"
       "VOLTDB_ROOT_DIR:        ${VOLTDB_ROOT_DIR}"
       "${PROJECT_NAME}:        PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}"
       "${PROJECT_NAME}:        PROJECT_BINARY_DIRECTORY is ${${PROJECT_NAME}_BINARY_DIR}"
       "VOLTDB_BUILD_TYPE:      ${VOLTDB_BUILD_TYPE}"
       "VOLT_LOG_LEVEL:         ${VOLT_LOG_LEVEL}"
       "VOLT_POOL_CHECKING:     ${VOLT_POOL_CHECKING}"
       "VOLT_TRACE_ALLOCATIONS: ${VOLT_TRACE_ALLOCATIONS}"
       "VOLT_TIMER_ENABLED:     ${VOLT_TIMER_ENABLED}"
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
# 1. Object files generally go into these folders.  The variables are
#    set in the root CMakeLists.txt file.
#      ${${PROJECT_NAME}_BINARY_DIR}
#                           Root of the output tree.  This is
#                           generally obj/debug/ee, obj/release/ee
#                           or obj/memcheck/ee.
#                           It will have -profile or -coverage at the
#                           end if those options are enabled.
#      ${CMAKE_BINARY_DIR}/3pty_install
#                           All third party artifacts on which which
#                           voltdb depends go here, in lib or include.
#                           There are some other subfolders created
#                           by the third party install procedures,
#                           but we don't care about anything but the
#                           static libraries and includes.  Note that
#                           this is not the project directory, but
#                           in the parent project's directory.
########################################################################
########################################################################
#
# Go and build the VoltDB library and IPC command.
#
########################################################################
#
# This is a list of all source files for the EE itself.  The order is
# probably not important, but it's in alphabetical order, so why not try
# to keep it that way?
#
########################################################################
SET (VOLTDB_SRC
  catalog/catalog.cpp
  catalog/catalogtype.cpp
  catalog/cluster.cpp
  catalog/column.cpp
  catalog/columnref.cpp
  catalog/constraint.cpp
  catalog/constraintref.cpp
  catalog/database.cpp
  catalog/function.cpp
  catalog/functionparameter.cpp
  catalog/index.cpp
  catalog/indexref.cpp
  catalog/materializedviewhandlerinfo.cpp
  catalog/materializedviewinfo.cpp
  catalog/planfragment.cpp
  catalog/statement.cpp
  catalog/table.cpp
  catalog/tableref.cpp
  catalog/topic.cpp
  catalog/property.cpp
  common/ExecuteWithMpMemory.cpp
  common/FatalException.cpp
  common/HiddenColumn.cpp
  common/InterruptException.cpp
  common/LargeTempTableBlockCache.cpp
  common/LoadTableCaller.cpp
  common/MiscUtil.cpp
  common/NValue.cpp
  common/PlannerDomValue.cpp
  common/Pool.cpp
  common/SQLException.cpp
  common/SegvException.cpp
  common/SerializableEEException.cpp
  common/StackTrace.cpp
  common/StreamPredicateList.cpp
  common/StringRef.cpp
  common/SynchronizedThreadLock.cpp
  common/ThreadLocalPool.cpp
  common/Topend.cpp
  common/TupleOutputStream.cpp
  common/TupleOutputStreamProcessor.cpp
  common/TupleSchema.cpp
  common/UndoLog.cpp
  common/UndoReleaseAction.cpp
  common/ValueFactory.cpp
  common/debuglog.cpp
  common/executorcontext.cpp
  common/serializeio.cpp
  common/tabletuple.cpp
  common/types.cpp
  execution/ExecutorVector.cpp
  execution/FragmentManager.cpp
  execution/JNITopend.cpp
  execution/ProgressMonitorProxy.cpp
  execution/VoltDBEngine.cpp
  executors/OptimizedProjector.cpp
  executors/abstractexecutor.cpp
  executors/abstractjoinexecutor.cpp
  executors/aggregateexecutor.cpp
  executors/commontableexecutor.cpp
  executors/deleteexecutor.cpp
  executors/executorfactory.cpp
  executors/executorutil.cpp
  executors/indexcountexecutor.cpp
  executors/indexscanexecutor.cpp
  executors/insertexecutor.cpp
  executors/largeorderbyexecutor.cpp
  executors/limitexecutor.cpp
  executors/materializedscanexecutor.cpp
  executors/materializeexecutor.cpp
  executors/mergereceiveexecutor.cpp
  executors/mergejoinexecutor.cpp
  executors/migrateexecutor.cpp
  executors/nestloopexecutor.cpp
  executors/nestloopindexexecutor.cpp
  executors/orderbyexecutor.cpp
  executors/projectionexecutor.cpp
  executors/receiveexecutor.cpp
  executors/sendexecutor.cpp
  executors/seqscanexecutor.cpp
  executors/swaptablesexecutor.cpp
  executors/tablecountexecutor.cpp
  executors/tuplescanexecutor.cpp
  executors/unionexecutor.cpp
  executors/updateexecutor.cpp
  executors/windowfunctionexecutor.cpp
  expressions/abstractexpression.cpp
  expressions/expressionutil.cpp
  expressions/functionexpression.cpp
  expressions/geofunctions.cpp
  expressions/operatorexpression.cpp
  expressions/parametervalueexpression.cpp
  expressions/scalarvalueexpression.cpp
  expressions/subqueryexpression.cpp
  expressions/tupleaddressexpression.cpp
  indexes/CoveringCellIndex.cpp
  indexes/IndexStats.cpp
  indexes/tableindex.cpp
  indexes/tableindexfactory.cpp
  topics/GroupStore.cpp
  topics/messages/OffsetCommit.cpp
  topics/messages/OffsetFetch.cpp
  topics/encode/AvroEncoder.cpp
  topics/encode/CsvEncoder.cpp
  topics/orm/Group.cpp
  topics/orm/GroupOffset.cpp
  topics/orm/GroupMember.cpp
  topics/orm/GroupOrmBase.cpp
  topics/TableFactory.cpp
  logging/JNILogProxy.cpp
  logging/LogManager.cpp
  plannodes/SchemaColumn.cpp
  plannodes/abstractjoinnode.cpp
  plannodes/abstractoperationnode.cpp
  plannodes/abstractplannode.cpp
  plannodes/abstractreceivenode.cpp
  plannodes/abstractscannode.cpp
  plannodes/aggregatenode.cpp
  plannodes/commontablenode.cpp
  plannodes/deletenode.cpp
  plannodes/indexcountnode.cpp
  plannodes/indexscannode.cpp
  plannodes/insertnode.cpp
  plannodes/limitnode.cpp
  plannodes/materializedscanplannode.cpp
  plannodes/materializenode.cpp
  plannodes/mergejoinnode.cpp
  plannodes/mergereceivenode.cpp
  plannodes/migratenode.cpp
  plannodes/nestloopindexnode.cpp
  plannodes/nestloopnode.cpp
  plannodes/orderbynode.cpp
  plannodes/plannodefragment.cpp
  plannodes/plannodeutil.cpp
  plannodes/projectionnode.cpp
  plannodes/receivenode.cpp
  plannodes/sendnode.cpp
  plannodes/seqscannode.cpp
  plannodes/swaptablesnode.cpp
  plannodes/tablecountnode.cpp
  plannodes/tuplescannode.cpp
  plannodes/unionnode.cpp
  plannodes/updatenode.cpp
  plannodes/windowfunctionnode.cpp
  stats/StatsAgent.cpp
  stats/StatsSource.cpp
  storage/AbstractDRTupleStream.cpp
  storage/BinaryLogSink.cpp
  storage/BinaryLogSinkWrapper.cpp
  storage/ConstraintFailureException.cpp
  storage/CopyOnWriteContext.cpp
  storage/CopyOnWriteIterator.cpp
  storage/DRTableNotFoundException.cpp
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
  storage/PersistentTableStats.cpp
  storage/StreamedTableStats.cpp
  storage/TableCatalogDelegate.cpp
  storage/TableStats.cpp
  storage/TableStreamer.cpp
  storage/TableStreamerContext.cpp
  storage/TableTupleAllocator.cpp
  storage/TempTableLimits.cpp
  storage/TopicTupleStream.cpp
  storage/TupleBlock.cpp
  storage/constraintutil.cpp
  storage/persistenttable.cpp
  storage/SystemTableFactory.cpp
  storage/streamedtable.cpp
  storage/table.cpp
  storage/tablefactory.cpp
  storage/tabletuplefilter.cpp
  storage/tableutil.cpp
  storage/temptable.cpp
  structures/CompactingPool.cpp
  structures/ContiguousAllocator.cpp
)

########################################################################
#
# This is a list of all third party sources we include in our
# shared library.  Keep it in alphabetical order, ok?
#
########################################################################
SET (VOLTDB_THIRD_PARTY_SRC
  ${CMAKE_SOURCE_DIR}/third_party/cpp/crc/crc32c.cc
  ${CMAKE_SOURCE_DIR}/third_party/cpp/crc/crc32ctables.cc
  ${CMAKE_SOURCE_DIR}/third_party/cpp/jsoncpp/json_reader.cpp
  ${CMAKE_SOURCE_DIR}/third_party/cpp/jsoncpp/json_value.cpp
  ${CMAKE_SOURCE_DIR}/third_party/cpp/jsoncpp/json_writer.cpp
  ${CMAKE_SOURCE_DIR}/third_party/cpp/murmur3/MurmurHash3.cpp
  ${CMAKE_SOURCE_DIR}/third_party/cpp/sha1/sha1.cpp
)

########################################################################
#
# These are sources for the jni and ipc drivers.  Only one will
# be used at a time.
#
########################################################################
SET (VOLTDB_JNI_SRC
  voltdbjni.cpp
  )
SET (VOLTDB_IPC_SRC
  voltdbipc.cpp
)

#
# These are sets of object files, something like static libraries.
#
VOLTDB_ADD_LIBRARY(voltdbobjs OBJECT ${VOLTDB_SRC})
VOLTDB_ADD_LIBRARY(voltdbjniobjs OBJECT ${VOLTDB_JNI_SRC})
VOLTDB_ADD_LIBRARY(voltdbipcobjs OBJECT ${VOLTDB_IPC_SRC})
VOLTDB_ADD_LIBRARY(third_party_objs OBJECT ${VOLTDB_THIRD_PARTY_SRC})

ADD_DEPENDENCIES(voltdbobjs s2geo crypto pcre2)
ADD_DEPENDENCIES(voltdbjniobjs s2geo crypto pcre2)
ADD_DEPENDENCIES(voltdbipcobjs s2geo crypto pcre2)

FOREACH (obj voltdbobjs voltdbjniobjs voltdbipcobjs)
   TARGET_INCLUDE_DIRECTORIES(${obj}
     PUBLIC
     ${CMAKE_SOURCE_DIR}/third_party/cpp
     ${${PROJECT_NAME}_SOURCE_DIR}
     ${VOLTDB_3PTY_INSTALL_PREFIX}/include)
ENDFOREACH()
TARGET_INCLUDE_DIRECTORIES(third_party_objs
  PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp
  ${${PROJECT_NAME}_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/third_party/cpp
  ${VOLTDB_3PTY_INSTALL_PREFIX}/include)

########################################################################
#
# Adding the voltdb jni library target.
#
########################################################################
SET (VOLTDB_LIBNAME voltdb-${VOLTDB_VERSION})
VOLTDB_ADD_LIBRARY(${VOLTDB_LIBNAME}
  SHARED
  $<TARGET_OBJECTS:voltdbobjs>
  $<TARGET_OBJECTS:voltdbjniobjs>
  $<TARGET_OBJECTS:third_party_objs>
  )

STRING(TOLOWER ${VOLTDB_BUILD_TYPE} obj_subdir)
TARGET_LINK_LIBRARIES(${VOLTDB_LIBNAME}
  -L${VOLTDB_3PTY_INSTALL_PREFIX}/lib
  -lpcre2-8 -ls2geo -lcrypto
  ${VOLTDB_LINK_FLAGS})

ADD_DEPENDENCIES(${VOLTDB_LIBNAME} s2geo pcre2 crypto)
########################################################################
#
# Adding the voltdbipc executable target.
#
########################################################################
SET (VOLTDB_IPCRUN_NAME voltdbipc)
VOLTDB_ADD_EXECUTABLE(${VOLTDB_IPCRUN_NAME}
  $<TARGET_OBJECTS:voltdbobjs>
  $<TARGET_OBJECTS:third_party_objs>
  $<TARGET_OBJECTS:voltdbipcobjs>
)

TARGET_LINK_LIBRARIES(${VOLTDB_IPCRUN_NAME}
  ${VOLTDB_LINK_FLAGS}
  -L${VOLTDB_3PTY_INSTALL_PREFIX}/lib
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
  DEPENDS ${VOLTDB_LIBNAME} ${VOLTDB_IPCRUN_NAME}
  )
