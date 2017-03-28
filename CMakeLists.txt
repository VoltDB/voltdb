CMAKE_MINIMUM_REQUIRED (VERSION 2.8.12 FATAL_ERROR)
PROJECT(VoltDB_EE)

########################################################################
#
# Options.
# These can be turned on and off to steer the build.
#
########################################################################
OPTION(USE_COVERAGE "Build for coverage analysis" OFF)
OPTION(USE_PROFILING "Build for profiling analysis" OFF)

########################################################################
#
# Names of folders in the tree, relative to
# the root.
#
########################################################################
SET(VOLTDB_EE_SRC_DIR src/ee)
SET(VOLTDB_THIRD_PARTY_CPP_DIR third_party/cpp
SET(THIRD_PARTY_INSTALL_DIR ${CMAKE_BINARY_DIR}/3pty-install)

EXECUTE_PROCESS(COMMAND cat ${CMAKE_SOURCE_DIR}/version.txt OUTPUT_VARIABLE VOLTDB_VERSION)

SET(VOLT_LOG_LEVEL 500 CACHE STRING "Log Level")

########################################################################
#
# Go and build the openssl library.  We only use this
# for the arbitrary precision arithmetic code, which
# S2GEO uses.
#
########################################################################
SET(LIBCRYPTO ${THIRD_PARTY_INSTALL_DIR}/lib/libcrypto.a)
SET(LIBCRYPTO_INCLUDES ${THIRD_PARTY_INSTALL_DIR}/include/openssl))
SET(OPENSSL_VERSION 1.0.2d)
SET(OPENSSL_SRC_DIR ${CMAKE_SOURCE_DIR}/${VOLTDB_THIRD_PARTY_CPP_DIR}/openssl/openssl-${OPENSSL_VERSION})
ADD_CUSTOM_COMMAND(OUTPUT ${LIBCRYPTO} ${LIBCRYPTO_INCLUDES}
  COMMAND ./config --prefix=${THIRD_PARTY_INSTALL_DIR}
  COMMAND make install
  WORKING_DIRECTORY ${OPENSSL_SRC_DIR})

########################################################################
#
# Go and build the S2GEO library.
#
########################################################################
ADD_SUBDIRECTORY(third_party/cpp/google-s2-geometry google-s2-geometry)

########################################################################
#
# Go and build the pcre2 library.
#
########################################################################

########################################################################
#
# Go and build the VoltDB library and IPC command.
#
########################################################################

SET(VOLTDB_EE_BASE_OPTS
  -Wall -Wextra -Werror -Woverloaded-virtual
  -Wpointer-arith -Wcast-qual -Wwrite-strings
  -Winit-self -Wno-sign-compare -Wno-unused-parameter
  -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK
  -fno-omit-frame-pointer
  -fvisibility=default
  -DBOOST_SP_DISABLE_THREADS -DBOOST_DISABLE_THREADS -DBOOST_ALL_NO_LIB
  -Wno-deprecated-declarations  -Wno-unknown-pragmas
  -DVOLT_LOG_LEVEL=${VOLT_LOG_LEVEL}
)

SET(VOLTDB_SRC
  ${VOLTDB_EE_SRC_DIR}/voltdbjni.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/catalog.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/catalogtype.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/cluster.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/column.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/columnref.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/connector.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/connectortableinfo.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/connectorproperty.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/constraint.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/constraintref.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/database.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/index.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/indexref.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/materializedviewhandlerinfo.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/materializedviewinfo.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/planfragment.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/statement.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/table.cpp
  ${VOLTDB_EE_SRC_DIR}/catalog/tableref.cpp
  ${VOLTDB_EE_SRC_DIR}/structures/ContiguousAllocator.cpp
  ${VOLTDB_EE_SRC_DIR}/common/FatalException.cpp
  ${VOLTDB_EE_SRC_DIR}/common/ThreadLocalPool.cpp
  ${VOLTDB_EE_SRC_DIR}/common/SegvException.cpp
  ${VOLTDB_EE_SRC_DIR}/common/SerializableEEException.cpp
  ${VOLTDB_EE_SRC_DIR}/common/SQLException.cpp
  ${VOLTDB_EE_SRC_DIR}/common/InterruptException.cpp
  ${VOLTDB_EE_SRC_DIR}/common/StringRef.cpp
  ${VOLTDB_EE_SRC_DIR}/common/tabletuple.cpp
  ${VOLTDB_EE_SRC_DIR}/common/TupleSchema.cpp
  ${VOLTDB_EE_SRC_DIR}/common/types.cpp
  ${VOLTDB_EE_SRC_DIR}/common/UndoLog.cpp
  ${VOLTDB_EE_SRC_DIR}/common/NValue.cpp
  ${VOLTDB_EE_SRC_DIR}/common/RecoveryProtoMessage.cpp
  ${VOLTDB_EE_SRC_DIR}/common/RecoveryProtoMessageBuilder.cpp
  ${VOLTDB_EE_SRC_DIR}/common/executorcontext.cpp
  ${VOLTDB_EE_SRC_DIR}/common/serializeio.cpp
  ${VOLTDB_EE_SRC_DIR}/common/StreamPredicateList.cpp
  ${VOLTDB_EE_SRC_DIR}/common/Topend.cpp
  ${VOLTDB_EE_SRC_DIR}/common/TupleOutputStream.cpp
  ${VOLTDB_EE_SRC_DIR}/common/TupleOutputStreamProcessor.cpp
  ${VOLTDB_EE_SRC_DIR}/common/MiscUtil.cpp
  ${VOLTDB_EE_SRC_DIR}/common/debuglog.cpp
  ${VOLTDB_EE_SRC_DIR}/execution/FragmentManager.cpp
  ${VOLTDB_EE_SRC_DIR}/execution/JNITopend.cpp
  ${VOLTDB_EE_SRC_DIR}/execution/VoltDBEngine.cpp
  ${VOLTDB_EE_SRC_DIR}/execution/ExecutorVector.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/OptimizedProjector.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/abstractexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/abstractjoinexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/aggregateexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/deleteexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/executorfactory.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/executorutil.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/indexcountexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/indexscanexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/insertexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/limitexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/materializedscanexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/materializeexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/mergereceiveexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/nestloopexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/nestloopindexexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/orderbyexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/windowfunctionexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/projectionexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/receiveexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/sendexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/seqscanexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/swaptablesexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/tablecountexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/tuplescanexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/unionexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/executors/updateexecutor.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/abstractexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/expressionutil.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/functionexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/geofunctions.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/operatorexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/parametervalueexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/scalarvalueexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/subqueryexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/tupleaddressexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/expressions/vectorexpression.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/abstractjoinnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/abstractoperationnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/abstractplannode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/abstractreceivenode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/abstractscannode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/aggregatenode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/deletenode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/indexscannode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/indexcountnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/tablecountnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/insertnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/limitnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/materializenode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/materializedscanplannode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/mergereceivenode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/nestloopindexnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/nestloopnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/orderbynode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/plannodefragment.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/plannodeutil.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/windowfunctionnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/projectionnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/receivenode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/SchemaColumn.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/sendnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/seqscannode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/swaptablesnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/tuplescannode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/unionnode.cpp
  ${VOLTDB_EE_SRC_DIR}/plannodes/updatenode.cpp
  ${VOLTDB_EE_SRC_DIR}/indexes/CoveringCellIndex.cpp
  ${VOLTDB_EE_SRC_DIR}/indexes/IndexStats.cpp
  ${VOLTDB_EE_SRC_DIR}/indexes/tableindex.cpp
  ${VOLTDB_EE_SRC_DIR}/indexes/tableindexfactory.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/AbstractDRTupleStream.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/BinaryLogSink.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/BinaryLogSinkWrapper.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/ConstraintFailureException.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/constraintutil.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/CopyOnWriteContext.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/CopyOnWriteIterator.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/DRTupleStream.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/ElasticContext.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/ElasticIndex.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/ElasticIndexReadContext.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/ElasticScanner.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/ExportTupleStream.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/MaterializedViewHandler.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/MaterializedViewTriggerForInsert.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/MaterializedViewTriggerForWrite.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/persistenttable.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/PersistentTableStats.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/RecoveryContext.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/streamedtable.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/StreamedTableStats.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/table.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TableCatalogDelegate.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/tablefactory.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TableStats.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TableStreamer.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TableStreamerContext.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/tableutil.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/tabletuplefilter.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/temptable.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TempTableLimits.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TupleBlock.cpp
  ${VOLTDB_EE_SRC_DIR}/storage/TupleStreamBase.cpp
  ${VOLTDB_EE_SRC_DIR}/stats/StatsAgent.cpp
  ${VOLTDB_EE_SRC_DIR}/stats/StatsSource.cpp
  ${VOLTDB_EE_SRC_DIR}/logging/JNILogProxy.cpp
  ${VOLTDB_EE_SRC_DIR}/logging/LogManager.cpp
)

SET (third_party_src
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/third_party/cpp/jsoncpp/jsoncpp.cpp
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/third_party/cpp/crc/crc32c.cc
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/third_party/cpp/crc/crc32ctables.cc
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/third_party/cpp/murmur3/MurmurHash3.cpp
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/third_party/cpp/sha1/sha1.cpp
)

ADD_LIBRARY(voltdbobjs OBJECT ${VOLTDB_SRC})
ADD_LIBRARY(third_party_objs OBJECT ${THIRD_PARTY_SRC})

TARGET_INCLUDE_DIRECTORIES(voltdbobjs PUBLIC ${VOLTDB_EE_SRC_DIR} ${VOLTDB_THIRD_PARTY_CPP_DIR})
TARGET_INCLUDE_DIRECTORIES(third_party_objs PUBLIC ${VOLTDB_THIRD_PARTY_CPP_DIR})

TARGET_COMPILE_OPTIONS(voltdbobjs ${VOLTDB_EE_OPTS})
TARGET_COMPILE_OPTIONS(third_party_objs ${THIRD_PARTY_OPTS})

ADD_DEPENDENCIES(voltdbobjs s2geo)
