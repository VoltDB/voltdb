CMAKE_MINIMUM_REQUIRED (VERSION 2.8.12 FATAL_ERROR)
PROJECT(VoltDB_EE)
INCLUDE(ExternalProject)
########################################################################
#
# This CMakeLists.txt file describes the VoltDB build system.
#
# There are a few rules which may make things be more organized.
# 1. All variables defined here should start with VOLTDB_, with a few
#    exceptions for historical reasons.  These exceptions are:
#          VOLT_LOG_LEVEL   See values VOLT_LEVEL_* in src/ee/common/debuglog.h
#                           The default is 500.  This is cached, so it
#                           needs to be specified to cmake each time it needs
#                           to be changed.
# 2. Object files generally go into these folders.
#      ${CMAKE_BINARY_DIR}  Root of the output tree.  This is
#                           generally obj/debug, obj/release or obj/memcheck.
#                           It will have -profile or -coverage if
#                           those options are enabled.
#       ${VOLTDB_THIRD_PARTY_INSTALL_DIR}
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
#
########################################################################
OPTION(VOLTDB_USE_COVERAGE "Build for coverage analysis" OFF)
OPTION(VOLTDB_USE_PROFILING "Build for profiling analysis" OFF)

########################################################################
#
# Sanity check the configuration.
#
########################################################################
IF (NOT DEFINED VOLTDB_BUILD_TYPE)
  MESSAGE (FATAL ERROR "VOLTDB_BUILD_TYPE is not defined.")
ENDIF ()
STRING(TOUPPER "${VOLTDB_BUILD_TYPE}" VOLTDB_BUILD_TYPE)
IF ((VOLTDB_BUILD_TYPE STREQUAL "MEMCHECK") OR (VOLTDB_BUILD_TYPE STREQUAL "MEMCHECK_NOFREELIST"))
  ADD_COMPILE_OPTIONS(-DDEBUG -DMEMCHECK )
ELSEIF (VOLTDB_BUILD_TYPE STREQUAL "DEBUG")
  ADD_COMPILE_OPTIONS(-g3 -DDEBUG)
ELSEIF (VOLTDB_BUILD_TYPE STREQUAL "RELEASE")
  ADD_COMPILE_OPTIONS(-O3 -mmmx -msse -msse2 -msse3 -DNDEBUG)
ELSE()
  MESSAGE(FATAL_ERROR "BUILD TYPE ${VOLTDB_BUILD_TYPE} IS UNKNOWN.")
ENDIF()
MESSAGE ("CMAKE_SOURCE_DIR: ${CMAKE_SOURCE_DIR}")
MESSAGE ("CMAKE_BINARY_DIR: ${CMAKE_BINARY_DIR}")
########################################################################
#
# Names of folders in the tree
#
########################################################################
# This is for our sources, relative to the source directory.
SET (VOLTDB_EE_SRC_DIR src/ee)
# This is for third party sources, relative to the source directory.
SET (VOLTDB_THIRD_PARTY_CPP_DIR third_party/cpp)
# This is where we install third party artifacts.
SET (VOLTDB_THIRD_PARTY_INSTALL_DIR ${CMAKE_BINARY_DIR}/3pty-install)
SET (VOLTDB_THIRD_PARTY_INCLUDE_DIR ${VOLTDB_THIRD_PARTY_INSTALL_DIR}/include)
# This where we build most third party artifacts.  OpenSSL is different.
SET (VOLTDB_THIRD_PARTY_OBJ_DIR ${CMAKE_BINARY_DIR}/3pty-obj)
# We keep pcre2 in a tarball, since we don't actually make
# any changes to it.  This keeps the git repository epsilon smaller.
SET (VOLTDB_TARBALLS ${CMAKE_SOURCE_DIR}/${VOLTDB_THIRD_PARTY_CPP_DIR}/tarballs)

# Fetch the version number.
EXECUTE_PROCESS(COMMAND cat ${CMAKE_SOURCE_DIR}/version.txt OUTPUT_VARIABLE VOLTDB_VERSION)
STRING(STRIP ${VOLTDB_VERSION} VOLTDB_VERSION)

# Set the log level.
SET (VOLT_LOG_LEVEL 500)

########################################################################
#  calculate the compiler versions, and the options needed
# for each of them. These are the versions of gcc and cmake for
# each version of Linux we support.
# OS Ver.        gcc vers     cmake ver.
# Centos6:          4.4.7     2.8.12.2
# Ubuntu 10.04      N.A.      N.A.
# Ubuntu 10.10      N.A.      N.A.
# Ubuntu 12.04      4.6.3     2.8.7
# Ubuntu 12.10      N.A.      N.A.
# Ubuntu 14.04      4.8.4     2.8.12.2
# Centos7:          4.8.5     2.8.12.2
# Ubuntu 14.10      N.A.      N.A.
# Ubuntu 15.04      4.9.2     3.0.2
# Ubunty 15.10      5.2.1     3.2.2
# Ubuntu 16.04      5.4.0     3.5.1
# Ubuntu 16.10      6.2.0     3.5.2
# Ubuntu 17.04      6.3.0     3.7.2
# Ubuntu 17.10      N.A.      N.A.
#
# We should have a similar table for the mac, but apparently we
# don't.  But we have some empirical evidence that some configurations
# will build and run correctly.
#
########################################################################
MESSAGE ("SYSTEM VERSION: ${CMAKE_SYSTEM}")
MESSAGE ("COMPILER VERSION: ${CMAKE_CXX_COMPILER_VERSION}")
SET (VOLTDB_COMPILER_U17p04 "6.3.0")
SET (VOLTDB_COMPILER_U16p10 "6.2.0")
SET (VOLTDB_COMPILER_U16p04 "5.4.0")
SET (VOLTDB_COMPILER_U15p10 "5.2.1")
SET (VOLTDB_COMPILER_U15p04 "4.9.2")
SET (VOLTDB_COMPILER_U14p04 "4.8.4")
SET (VOLTDB_COMPILER_C7     "4.8.5")
SET (VOLTDB_COMPILER_12p04  "4.6.3")
SET (VOLTDB_COMPILER_C6     "4.4.7")
SET (VOLTDB_COMPILER_OLDE   "4.4.0")
IF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  SET (VOLTDB_LINK_FLAGS ${VOLTDB_LINK_FLAGS} -pthread)
  SET (VOLTDB_IPC_LINK_FLAGS ${VOLTDB_LIB_LINK_FLAGS} -rdynamic)
  ADD_COMPILE_OPTIONS(-pthread -Wno-deprecated-declarations  -Wno-unknown-pragmas)
  # It turns out to be easier to go from a higher version to a lower
  # version, since we can't easily test <= and >=.
  IF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U17p04 )
    # COMPILER_VERSION > 6.3.0
    MESSAGE ("GCC Version ${CMAKE_CXX_COMPILER_VERSION} is not verified for building VoltDB.")
    MESSAGE ("We're using the options for 6.2.0, which is the newest one we've tried.  Good Luck.")
    ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U16p10)
    # 6.2.0 < COMPILER_VERSION and COMPILER_VERSION <= 6.3.0
    MESSAGE("Using the Ubuntu 17.04 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U16p04 )
    # 5.4.0 < COMPILER_VERSION and COMPILER_VERSION <= 6.2.0
    MESSAGE("Using the Ubuntu 16.10 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs )
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U15p10 )
    # 5.2.1 < COMPILER_VERSION and COMPILER_VERSION <= 5.4.0
    MESSAGE("Using the Ubuntu 16.04 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    ADD_COMPILE_OPTIONS( -Wno-unused-local-typedefs )
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U15p04  )
    # 4.9.2 < COMPILER_VERSION and COMPILER_VERSION <= 5.2.1
    MESSAGE("Using the Ubuntu 15.10 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    ADD_COMPILE_OPTIONS( -Wno-unused-local-typedefs )
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U14p04 )
    # 4.8.4 < COMPILER_VERSION and COMPILER_VERSION <= 4.9.2
    # Note that U14.04 and C7 are different versions, but equivalent
    # for our needs here.
    # Nothing special added to the compile flags.
    MESSAGE("Using the Ubuntu 15.04 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    ADD_COMPILE_OPTIONS(-Wno-unused-but-set-variable -Wno-unused-local-typedefs -Wno-float-conversion -Wno-conversion)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_CXX0X)
    # 4.6.0 < COMPILER_VERSION and COMPILER_VERSION <= 4.8.4
    # Use -std=c++0x.  This is in GCC's experimental C++11 compiler
    # support region.
    MESSAGE("Using the Centos 6 settings for ${CMAKE_CXX_COMPILER_VERSION}")
    ADD_COMPILE_OPTIONS(-Wno-unused-but-set-variable -Wno-unused-local-typedefs -Wno-float-conversion -Wno-conversion)
    SET (CXX_VERSION_FLAG -std=c++0x)
  ELSE()
    message(FATAL_ERROR "GNU Compiler version ${CMAKE_CXX_COMPILER_VERSION} is too old to build VoltdB.  Try at least 4.4.7.")
  ENDIF()
ELSEIF (CMAKE_CXX_COMPILER_ID STREQUAL "CLANG")
  # All clang is C++11.
  SET (CXX_VERSION_FLAG -std=c++11)
  if ( CMAKE_CXX_COMPILER_VERSION VERSION_LESS "3.5.0" )
    # Some clang 3.4.x version
  ELSEIF ( NOT ( "7.0.0" VERSION_LESS CMAKE_CXX_COMPILER_VERSION ) )
    # This is some odd mac version number.  It's not
    # related to the LLVM versioning numbers.
  ELSE()
    MESSAGE (FATAL_ERROR "Unknown compiler family ${CMAKE_CXX_COMPILER_ID}.  We only support gcc and clang.")
  ENDIF ()
ENDIF ()

ADD_COMPILE_OPTIONS(${CXX_VERSION_FLAG})

MESSAGE("SYSTEM NAME: ${CMAKE_SYSTEM_NAME}")
IF ( ${CMAKE_SYSTEM_NAME} STREQUAL "Linux" )
  ADD_COMPILE_OPTIONS(-Wno-attributes -Wcast-align -DLINUX -fpic)
  SET (VOLTDB_NM_OPTIONS "-n --demangle")
  IF (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    SET (VOLTDB_OPENSSL_TOKEN "linux-x86_64:gcc -fpic")
  ELSE()
    MESSAGE(FATAL_ERROR "Compiler family ${CMAKE_CXX_COMPILER_ID} is not supported when building VoltDB.")
  ENDIF()
ELSEIF( ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" )
  SET (VOLTDB_NM_OPTIONS "-n")
  SET (VOLTDB_OPENSSL_TOKEN "darwin64-x86_64-cc")
ELSE()
  MESSAGE(FATAL_ERROR "System nameed ${CMAKE_SYSTEM_NAME} is unknown")
ENDIF()

########################################################################
#
# Go and build the pcre2 library.
# We have to untar it and configure it.  We will
# do this at build time.
#
########################################################################
SET (VOLTDB_PCRE2_PREFIX ${VOLTDB_THIRD_PARTY_OBJ_DIR}/pcre2)
SET (VOLTDB_PCRE2_VERSION 10.10)
SET (VOLTDB_PCRE2_NAME pcre2-${VOLTDB_PCRE2_VERSION})
SET (VOLTDB_PCRE2_TARBALL ${VOLTDB_TARBALLS}/${VOLTDB_PCRE2_NAME}.tgz)
SET (VOLTDB_PCRE2_SRC ${VOLTDB_PCRE2_PREFIX}/src/${VOLTDB_PCRE2_NAME})
SET (VOLTDB_PCRE2_OBJ ${VOLTDB_PCRE2_PREFIX}/obj/${VOLTDB_PCRE2_NAME})

ExternalProject_Add(pcre2
  PREFIX ${VOLTDB_PCRE2_PREFIX}
  URL ${VOLTDB_PCRE2_TARBALL}
  SOURCE_DIR ${VOLTDB_PCRE2_SRC}
  BINARY_DIR ${VOLTDB_PCRE2_OBJ}
  CONFIGURE_COMMAND ${VOLTDB_PCRE2_SRC}/configure -disable-shared --with-pic --prefix=${VOLTDB_THIRD_PARTY_INSTALL_DIR}
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
SET (VOLTDB_OPENSSL_TARBALL ${VOLTDB_TARBALLS}/${VOLTDB_OPENSSL_NAME}.tgz)
SET (VOLTDB_OPENSSL_PREFIX ${VOLTDB_THIRD_PARTY_OBJ_DIR}/openssl)
# Binary and source are the same for openssl.  There
# are not obvious ways to do out-of-tree builds.
SET (VOLTDB_OPENSSL_SRC ${VOLTDB_OPENSSL_PREFIX}/src/${VOLTDB_OPENSSL_NAME})
SET (VOLTDB_OPENSSL_OBJ ${VOLTDB_OPENSSL_PREFIX}/src/${VOLTDB_OPENSSL_NAME})
ExternalProject_Add(crypto
  PREFIX ${VOLTDB_OPENSSL_PREFIX}
  URL ${VOLTDB_OPENSSL_TARBALL}
  SOURCE_DIR ${VOLTDB_OPENSSL_SRC}
  BINARY_DIR ${VOLTDB_OPENSSL_OBJ}
  CONFIGURE_COMMAND ./Configure --prefix=${VOLTDB_THIRD_PARTY_INSTALL_DIR} ${VOLTDB_OPENSSL_TOKEN}
  BUILD_COMMAND make
  INSTALL_COMMAND make install)

########################################################################
#
# Go and build the S2GEO library.
#
########################################################################
SET (VOLTDB_S2GEO_SRC ${VOLTDB_THIRD_PARTY_CPP_DIR}/google-s2-geometry)
SET (VOLTDB_S2GEO_OBJ ${VOLTDB_THIRD_PARTY_OBJ_DIR}/google-s2-geometry)
SET (VOLTDB_S2GEO_CMAKE_CONFIG
  -DCXX_VERSION_FLAG=${CXX_VERSION_FLAG}
  -DVOLTDB_THIRD_PARTY_CPP_DIR=${CMAKE_SOURCE_DIR}/${VOLTDB_THIRD_PARTY_CPP_DIR}
  -DCMAKE_INSTALL_PREFIX=${VOLTDB_THIRD_PARTY_INSTALL_DIR}
  -DCMAKE_BUILD_TYPE=${VOLTDB_BUILD_TYPE})
SET (VOLTDB_S2GEO_PREFIX ${VOLTDB_THIRD_PARTY_OBJ_DIR})
ExternalProject_Add(s2geo
  DEPENDS crypto
  PREFIX ${VOLTDB_S2GEO_PREFIX}
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/${VOLTDB_S2GEO_SRC}
  CMAKE_ARGS ${VOLTDB_S2GEO_CMAKE_CONFIG}
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  BINARY_DIR ${VOLTDB_S2GEO_OBJ})
########################################################################
#
# Go and build the VoltDB library and IPC command.
#
########################################################################
#
# These are the base compiler options.
#
########################################################################
ADD_COMPILE_OPTIONS(
  -Wall -Wextra -Werror -Woverloaded-virtual
  -Wpointer-arith -Wcast-qual -Wwrite-strings
  -Winit-self -Wno-sign-compare -Wno-unused-parameter
  -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK
  -fno-omit-frame-pointer
  -fvisibility=default
  -DBOOST_SP_DISABLE_THREADS -DBOOST_DISABLE_THREADS -DBOOST_ALL_NO_LIB
  -Wno-deprecated-declarations  -Wno-unknown-pragmas
  -Wno-ignored-qualifiers -fno-strict-aliasing
  -DVOLT_LOG_LEVEL=${VOLT_LOG_LEVEL}
)

# Set coverage and profiling options
IF ( ${VOLTDB_USE_COVERAGE} )
  SET (VOLTDB_LINK_FLAGS "${VOLTDB_LINK_FLAGS} -ftest-coverage -fprofile-arcs")
ENDIF ()

IF ( ${VOLTDB_USE_PROFILING} )
  SET (VOLTDB_LINK_FLAGS "${VOLTDB_LINK_FLAGS} -L/usr/local/lib -g3 -lprofiler -lunwind")
ENDIF ()

SET (VOLTDB_SRC
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

SET (VOLTDB_THIRD_PARTY_SRC
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/jsoncpp/jsoncpp.cpp
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/crc/crc32c.cc
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/crc/crc32ctables.cc
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/murmur3/MurmurHash3.cpp
  ${VOLTDB_THIRD_PARTY_CPP_DIR}/sha1/sha1.cpp
)

SET (VOLTDB_JNI_SRC
  ${VOLTDB_EE_SRC_DIR}/voltdbjni.cpp
  )
SET (VOLTDB_IPC_SRC
  ${VOLTDB_EE_SRC_DIR}/voltdbipc.cpp
)
# MESSAGE("VOLTDB_SRC: ${VOLTDB_SRC}")
# MESSAGE("VOLTDB_THIRD_PARTY_SRC: ${VOLTDB_THIRD_PARTY_SRC}")
# MESSAGE("VOLTDB_IPC_SRC: ${VOLTDB_IPC_SRC}")
ADD_LIBRARY(voltdbobjs OBJECT ${VOLTDB_SRC})
ADD_LIBRARY(voltdbjniobjs OBJECT ${VOLTDB_JNI_SRC})
ADD_LIBRARY(voltdbipcobjs OBJECT ${VOLTDB_IPC_SRC})
ADD_LIBRARY(third_party_objs OBJECT ${VOLTDB_THIRD_PARTY_SRC})

ADD_DEPENDENCIES(voltdbobjs s2geo crypto pcre2)
ADD_DEPENDENCIES(voltdbipcobjs s2geo crypto pcre2)
ADD_DEPENDENCIES(voltdbjniobjs s2geo crypto pcre2)

MESSAGE("VOLTDB_EE_SRC_DIR: ${VOLTDB_EE_SRC_DIR}")
MESSAGE("VOLTDB_THIRD_PARTY_CPP_DIR: ${VOLTDB_THIRD_PARTY_CPP_DIR}")
MESSAGE("VOLTDB_THIRD_PARTY_INCLUDE_DIR: ${VOLTDB_THIRD_PARTY_INCLUDE_DIR}")
MESSAGE("VOLTDB_THIRD_PARTY_INCLUDE_DIR: ${VOLTDB_THIRD_PARTY_INCLUDE_DIR}")
TARGET_INCLUDE_DIRECTORIES(voltdbobjs
  PUBLIC
  ${VOLTDB_EE_SRC_DIR}
  ${VOLTDB_THIRD_PARTY_CPP_DIR}
  ${VOLTDB_THIRD_PARTY_INCLUDE_DIR})
TARGET_INCLUDE_DIRECTORIES(voltdbipcobjs
  PUBLIC
  ${VOLTDB_EE_SRC_DIR}
  ${VOLTDB_THIRD_PARTY_CPP_DIR}
  ${VOLTDB_THIRD_PARTY_INCLUDE_DIR})
TARGET_INCLUDE_DIRECTORIES(voltdbjniobjs
  PUBLIC
  ${VOLTDB_EE_SRC_DIR}
  ${VOLTDB_THIRD_PARTY_CPP_DIR}
  ${VOLTDB_THIRD_PARTY_INCLUDE_DIR})
TARGET_INCLUDE_DIRECTORIES(third_party_objs
  PUBLIC
  ${VOLTDB_THIRD_PARTY_CPP_DIR})

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
  -L${VOLTDB_THIRD_PARTY_INSTALL_DIR}/lib
  -lpcre2-8 -ls2geo -lcrypto
  ${VOLTDB_LINK_FLAGS}
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
  -L${VOLTDB_THIRD_PARTY_INSTALL_DIR}/lib
  -lpcre2-8 -ls2geo -lcrypto
  ${VOLTDB_LINK_FLAGS}
  -ldl)

INSTALL(TARGETS ${VOLTDB_LIBNAME} ${VOLTDB_IPCRUN_NAME}
  DESTINATION ${CMAKE_SOURCE_DIR}/prod)

########################################################################
#
# What about voltrun?  Do we use that anymore?  Where is it used?
#
########################################################################
