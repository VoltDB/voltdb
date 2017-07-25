########################################################################
#
# A CMake build configuration for VoltDB.  The commands for the ant
# targets in build.xml call src/ee/tools/build_cmake.py, which calls
# cmake and make here, so you don't really need to know how to use this.
# But for special cases this may be useful.
#
# To use this standalone, in the root of the voltdb hierarchy, run this
# procedure. All files are relative to the VoltDB source tree root.  This
# is the directory which contains src, test, voltdb and the VoltDB
# LICENSE file.
#   1.) mkdir -p obj/${BUILD_TYPE}
#       where ${BUILD_TYPE} is memcheck, release or debug.
#   2.) cd obj/${BUILD_TYPE}
#   3.) cmake -DVOLTDB_BUILD_TYPE=${BUILD_TYPE} ${SRC_DIR}
#       where ${SRC_DIR} is the pathname of the voltdb
#       source directory, typically ../../voltdb/src/ee  There are other
#       arguments which could be added.  See build.xml.  (The
#       ../.. is there because we have changed directories to
#       obj/${BUILD_TYPE}.  CMake looks at the build environment
#       and creates makefiles aplenty.
#   4.) make -j<N> -k install
#       where, as usual, <N> is number telling make how much parallelism
#       to use.  Values between 1 and 8 are typical.  Higher values
#       may produce faster builds, or they may just gum things up.
#       If you use N=50 you will have to reboot your computer.  Really.
#   5.) If this succeeds, then the build artifacts will be placed in
#       obj/${BUILD}/nativelibs and obj/${BUILD}/prod.
#   6.) To build the tests, see the section labelled Tests below.
#
# Note that there's the potential for making Eclipse projects here.
# Also, there's the potential for substituting ninja for make, which
# would improve EE build times.  More experience is needed.
#
# The two artifacts are:
#   1.) The jni library, which will be in
#           nativelibs/libvoltdb-VERSION.so
#       for Linux and
#           nativelibs/libvoltdb-VERSION.dylib
#       for Darwin.
#   2.) The voltdbipcrun program, which is used for memory leak detection,
#       and to debug this EE.  This will be found in prod/voltdbipcrun
#       on both Linux and Darwin.
#
########################################################################
#
# Tests
#
########################################################################
#
# In addition to making the libraries, this file describes all the
# C++ unit tests.
#   0.) Tests are either generated or hand written tests.
#       a.) The hand written tests are checked in to github,
#           and are listed in the variable VOLTDB_TEST_PROGRAMS
#           below.  Their names have the form TEST_DIR/TEST_NAME.
#           Both TEST_DIR and TEST_NAME should be simple names,
#           and not a paths.
#       b.) Generated tests are generated from java sources.
#           These are placed in a directory in the source tree
#           which is ignored by git.  Each time cmake is run we
#           look at whether to generate these.  If the result
#           of generating the file would not change the file's
#           contents we don't actually write the file, so as to
#           preserve the file's timestamp and not force a
#           recompilation.
#           i.) Generated sources are in tests/ee/generated/src
#               but tests/ee/generated is ignored by git through
#               a gitignore file.
#           ii.) Generated test binaries are placed in the same
#               place that hand written tests are placed
#       c.) Tests must all be unique by name.  We can't have
#           two tests in different directories with the same
#           name.  This simplifies some processing here.
#   1.) Building Tests
#       To build all the tests run "make build-all-tests".  You
#       can use the usual -jN and -k options, of course.
#       The tests, generated and hand written, will all go into
#       cpptests/TEST_DIR/TEST_NAME.
#
#       To build one test or all test in one test directory,
#       run "make build-ALPHA" where ALPHA is the name of
#       the test or the test directory.  Generated tests
#       are treated just like hand written tests, except that
#       their source is placed in tests/ee/generated/TEST_DIR,
#       not tests/ee/TEST_DIR.  CMake takes care of all of this.
#   2.) Running Tests
#       To run all the tests run "make run-all-tests."  For
#       memcheck builds this runs valgrind tests.  The full
#       valgrind command is printed.
#
#       To make one test executable run "make TESTEXENAME.  To make
#       all tests in a test directory, run "make run-ALPHA", where
#       ALPHA is the name of the test directory.  All directories will
#       be made, and dependences will be made.  Generated and
#       hand-written tests are treated identically here.
#
#       To run one test executable run "make run-TESTEXENAME".  To
#       run all tests in test directory ALPHA run "make run-ALPHA".
#       Generated and hand-written tests are treated identically here.
#       If any test executable is out of date it will be rebuilt.
#   4.) To delete any test binaries, just delete them.  Delete
#       cpptests to delete all test binaries.  The test system
#       will recreate any directories which need to be made.
#       Deleting tests/ee/generated has no effect, other than to
#       force the recreation of the generated sources.
#
########################################################################
#
# System Dependences
#
########################################################################
#
# Note that by default only GCC is supported on Linux, and only Clang is
# supported on Darwin.  By redefining CMAKE_CXX_COMPILER, in the usual
# CMake way, one can change this.  New C++ compilation flags can be
# added by defining the cmake variable VOLTDB_CXXFLAGS.  New libraries can be added
# to the link line for libvolt-*.so by defining VOLTDB_LINK_FLAGS
# and VOLTDB_LDFLAGS.  For example,
#    cmake -DVOLT_CXX_FLAGS="-fno-use-pirates" -DVOLT_LDFLAGS="-lninjutsu".
#
# This requires a gcc compiler version of at least 4.4.7, and a cmake
# version of at least 2.8.12.  It's possible that older versions of
# cmake will work, but 2.8.7, which is the released version for
# Ubuntu 12.04, will be too old.  Recompiling a more recent cmake
# version might very well make Ubuntu 12.04 a viable platform, but we
# have not tested this.
#
# We also use Google's S2 geography library, pcre2 for regular
# expression matching and openssl for arbitrary precision arithmetic.
# These are all included in the source code of VoltDB.  But pcre3
# is an autoconf/automake program.  So, you will need the autoconf
# and automake suites.  Trying to build without autotools gives obscure
# error messages, so make sure autotools is installed.
#
########################################################################

CMAKE_MINIMUM_REQUIRED (VERSION 2.8.12 FATAL_ERROR)
PROJECT(VoltDB_EE)
INCLUDE(ExternalProject)
IF ( (CMAKE_SYSTEM_NAME STREQUAL "Darwin") AND ( (CMAKE_VERSION VERSION_GREATER 3.0.0) OR (CMAKE_VERSION VERSION_EQUAL 3.0.0) ) )
    # Newer versions of CMake than we use set this
    # policy on the mac.  There there is a way to set the
    # rpath which is different from Linux.  We just ignore
    # it, but we need to use the old behavior on the mac.
    CMAKE_POLICY(SET CMP0042 OLD)
ENDIF()

########################################################################
#
# Some functions to make things look simpler.
#
########################################################################
INCLUDE(VoltDB.cmake)


COMPUTE_CORE_COUNT(VOLTDB_CORE_COUNT)
# MESSAGE("VOLTDB_CORE_COUNT IS ${VOLTDB_CORE_COUNT}")
########################################################################
#
# This CMakeLists.txt file describes the VoltDB build system.
#
# There are a few rules which may make things be more organized.
# 1. All variables defined here should start with VOLTDB_, with a few
#    exceptions for historical reasons.  These exceptions are:
#          VOLT_LOG_LEVEL   The numeric log level.  See the values 
#                           for VOLT_LEVEL_* in src/ee/common/debuglog.h
#                           The default is 500.  This is cached, so it
#                           needs to be specified to cmake each time it needs
#                           to be changed.
#          Some Local Variables when configuring tests.
# 2. Object files generally go into these folders.
#      ${CMAKE_BINARY_DIR}  Root of the output tree.  This is
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
# 3. Just to review the way CMake works:
#    a. CMake is best thought of as a functional language which describes
#       the artifacts to be built by describing their constituents.
#    b. Artifacts are called *targets*.  Targets can be libraries or
#       executables.  We can also add tests.  These three are created
#       with ADD_LIBRARY, ADD_EXECUTABLE and ADD_TEST.
#    c. There are three kinds of library, SHARED, STATIC and OBJECT.
#       SHARED is the usual dynamically linked library, denoted by a
#       .so or .dylib suffix.  STATIC libraries are the usual .a
#       archives, though we don't use these here.  OBJECT libraries,
#       which we do use, are just sets of .o object files which
#       CMake manages for us with a single name.
#    d. Targets have properties, like compilation flags, link libraries
#       and include directories.  These are associated with a target
#       using ADD_COMPILE_OPTIONS, TARGET_LINK_LIBRARIES and
#       TARGET_INCLUDE_DIRECTORIES.  Tests have names and other
#       properties.
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
# Sanity check the configuration.
#
########################################################################
IF (NOT DEFINED VOLTDB_BUILD_TYPE)
  MESSAGE (FATAL ERROR "VOLTDB_BUILD_TYPE is not defined.")
ENDIF ()

########################################################################
#
# Add some special flags if this is a valgrind build.
#
########################################################################
STRING(TOUPPER "${VOLTDB_BUILD_TYPE}" VOLTDB_BUILD_TYPE)
STRING(TOLOWER "${VOLTDB_BUILD_TYPE}" VOLTDB_BUILD_TYPE_LOWER)
IF ((${VOLTDB_BUILD_TYPE} STREQUAL "MEMCHECK") OR (${VOLTDB_BUILD_TYPE} STREQUAL "MEMCHECK_NOFREELIST"))
  SET (IS_VALGRIND_BUILD TRUE)
ELSE()
  SET(IS_VALGRIND_BUILD FALSE)
ENDIF()

########################################################################
#
# Set some special flags for valgrind.
#
########################################################################
IF (IS_VALGRIND_BUILD)
  ADD_COMPILE_OPTIONS(-DDEBUG -DMEMCHECK )
  SET (VOLTDB_USE_VALGRIND --valgrind)
ELSEIF (VOLTDB_BUILD_TYPE STREQUAL "DEBUG")
  ADD_COMPILE_OPTIONS(-g3 -DDEBUG)
ELSEIF (VOLTDB_BUILD_TYPE STREQUAL "RELEASE")
  ADD_COMPILE_OPTIONS(-O3 -mmmx -msse -msse2 -msse3 -DNDEBUG)
ELSE()
  MESSAGE(FATAL_ERROR "BUILD TYPE ${VOLTDB_BUILD_TYPE} IS UNKNOWN.")
ENDIF()
########################################################################
#
# It turns out that knowing the VOLTDB_ROOT is useful.  A typical
# CMake project has all files below one single directory, CMAKE_SOURCE_DIR.
# We set CMAKE_SOURCE_DIR to ${VOLTDB_ROOT}/src/ee, but there are test
# sources in ${VOLTDB_ROOT}/tests/ee and third party sources in
# ${VOLTDB_ROOT}/third_party.  So we need to know the names of folders
# outside of the CMAKE_SOURCE_DIR.
#
########################################################################
FIND_VOLTDB_ROOT(VOLTDB_ROOT)
# MESSAGE("VOLTDB_ROOT IS ${VOLTDB_ROOT}")

# We keep pcre2 in a tarball, since we don't actually make
# any changes to it.  This keeps the git repository epsilon smaller.

# Fetch the version number.
EXECUTE_PROCESS(COMMAND cat ${VOLTDB_ROOT}/version.txt OUTPUT_VARIABLE VOLTDB_VERSION)
STRING(STRIP ${VOLTDB_VERSION} VOLTDB_VERSION)

# Set the log level.
SET (VOLT_LOG_LEVEL 500)

########################################################################
#
# Compiler Options
#
########################################################################
#
# We give compiler options is two parts.  One is the set of base
# options, which all compilations use.  The second is the set of
# compiler, platform and version options.
#
# These are the base compiler options, and some
# other linker options.
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
  SET (VOLTDB_LINK_FLAGS ${VOLTDB_LINK_FLAGS} -ftest-coverage -fprofile-arcs)
ENDIF ()

IF ( ${VOLTDB_USE_PROFILING} )
  SET (VOLTDB_LINK_FLAGS ${VOLTDB_LINK_FLAGS} -L/usr/local/lib -g3 -lprofiler -lunwind)
ENDIF ()

#
# Add VOLTDB_LDFLAGS, which is supplied by the user.
#
SET (VOLTDB_LINK_FLAGS ${VOLTDB_LINK_FLAGS} ${VOLTDB_LDFLAGS})

########################################################################
# 
# These are the compiler version specific options. 
# We calculate the compiler versions, and the options needed
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
# don't.  We do have some empirical evidence that some configurations
# will build and run correctly.
#
########################################################################
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
MESSAGE("Using compiler ${CMAKE_CXX_COMPILER_ID}")
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
    ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-array-bounds)
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
ELSEIF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # All clang is C++11.
  SET (CXX_VERSION_FLAG -std=c++11)
  MESSAGE("CXX_VERSION_FLAG is ${CXX_VERSION_FLAG}")
  IF ( ( "3.4.0" VERSION_LESS CMAKE_CXX_COMPILER_VERSION ) 
       AND ( CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.0.0" ) )
    # Some clang 3.4.x version
    ADD_COMPILE_OPTIONS(-Wno-varargs)
  ELSEIF ( "7.0.0" VERSION_LESS ${CMAKE_CXX_COMPILER_VERSION} )
    # This is some odd mac version number.  It's not
    # related to the LLVM versioning numbers.
    ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-absolute-value)
  ENDIF()
ELSE()
  MESSAGE (FATAL_ERROR "Unknown compiler family ${CMAKE_CXX_COMPILER_ID}.  We only support GNU and Clang.")
ENDIF ()

ADD_COMPILE_OPTIONS(${CXX_VERSION_FLAG})

#
# Add VOLTDB_CXXFLAGS, supplied by the user.
#
ADD_COMPILE_OPTIONS(${VOLTDB_CXXFLAGS})

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
  ADD_COMPILE_OPTIONS("-DMACOSX")
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
SET (VOLTDB_PCRE2_PREFIX ${CMAKE_BINARY_DIR}/3pty-obj/pcre2)
SET (VOLTDB_PCRE2_VERSION 10.10)
SET (VOLTDB_PCRE2_NAME pcre2-${VOLTDB_PCRE2_VERSION})
SET (VOLTDB_PCRE2_TARBALL ${VOLTDB_ROOT}/third_party/cpp/tarballs/${VOLTDB_PCRE2_NAME}.tgz)
SET (VOLTDB_PCRE2_SRC ${VOLTDB_PCRE2_PREFIX}/src/${VOLTDB_PCRE2_NAME})
SET (VOLTDB_PCRE2_OBJ ${VOLTDB_PCRE2_PREFIX}/obj/${VOLTDB_PCRE2_NAME})

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
SET (VOLTDB_OPENSSL_TARBALL ${VOLTDB_ROOT}/third_party/cpp/tarballs/${VOLTDB_OPENSSL_NAME}.tgz)
SET (VOLTDB_OPENSSL_PREFIX ${CMAKE_BINARY_DIR}/3pty-obj/openssl)
# Binary and source are the same for openssl.  There
# are not obvious ways to do out-of-tree builds.
SET (VOLTDB_OPENSSL_SRC ${VOLTDB_OPENSSL_PREFIX}/src/${VOLTDB_OPENSSL_NAME})
SET (VOLTDB_OPENSSL_OBJ ${VOLTDB_OPENSSL_PREFIX}/src/${VOLTDB_OPENSSL_NAME})
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
SET (VOLTDB_S2GEO_SRC "${VOLTDB_ROOT}/third_party/cpp/google-s2-geometry")
SET (VOLTDB_S2GEO_OBJ ${CMAKE_BINARY_DIR}/3pty-obj/google-s2-geometry)
SET (VOLTDB_S2GEO_CMAKE_CONFIG
  -DCXX_VERSION_FLAG=${CXX_VERSION_FLAG}
  -DVOLTDB_THIRD_PARTY_CPP_DIR=${VOLTDB_ROOT}/third_party/cpp
  -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/3pty-install
  -DCMAKE_BUILD_TYPE=${VOLTDB_BUILD_TYPE})
SET (VOLTDB_S2GEO_PREFIX ${CMAKE_BINARY_DIR}/3pty-obj)

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
  ${VOLTDB_ROOT}/third_party/cpp/jsoncpp/jsoncpp.cpp
  ${VOLTDB_ROOT}/third_party/cpp/crc/crc32c.cc
  ${VOLTDB_ROOT}/third_party/cpp/crc/crc32ctables.cc
  ${VOLTDB_ROOT}/third_party/cpp/murmur3/MurmurHash3.cpp
  ${VOLTDB_ROOT}/third_party/cpp/sha1/sha1.cpp
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
  ${VOLTDB_ROOT}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdbobjs
  PUBLIC
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)
TARGET_INCLUDE_DIRECTORIES(voltdbipcobjs
  SYSTEM PUBLIC
  ${VOLTDB_ROOT}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdbipcobjs
  PUBLIC
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)
TARGET_INCLUDE_DIRECTORIES(voltdbjniobjs
  SYSTEM PUBLIC
  ${VOLTDB_ROOT}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdbjniobjs
  PUBLIC
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)
TARGET_INCLUDE_DIRECTORIES(third_party_objs
  SYSTEM PUBLIC
  ${VOLTDB_ROOT}/third_party/cpp)

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
########################################################################
#
# Testing
#
########################################################################
ENABLE_TESTING()
SET (VOLTDB_TEST_SRC ${VOLTDB_ROOT}/tests/ee)
SET (VOLTDB_TEST_DIR ${CMAKE_BINARY_DIR}/tests)
#
# This is a bit adhoc.
#
# All the tests in ${VOLTDB_FAILING_VALGRIND_DIRS} are
# expected to fail under valgrind except for those
# named in VOLTDB_PASSING_VALGRIND_TESTS.
#
SET (VOLTDB_FAILING_VALGRIND_DIRS memleaktests)
SET (VOLTDB_PASSING_VALGRIND_TESTS no_losses)
########################################################################
#
# Here we have all the names of tests which are not auto-generated.
# They have the form directory/test_name.  The test names must all be 
# globally unique.  We test for this later on.
#
########################################################################
SET (VOLTDB_MANUAL_TEST_PROGRAMS
  harness_test/harness_tester
  catalog/catalog_test
  logging/logging_test
  memleaktests/definite_losses
  memleaktests/indirect_losses
  memleaktests/still_reachable_losses
  memleaktests/possible_losses
  memleaktests/rw_deleted
  memleaktests/no_losses
  common/debuglog_test
  common/elastic_hashinator_test
  common/PerFragmentStatsTest
  common/nvalue_test
  common/pool_test
  common/serializeio_test
  common/tabletuple_test
  common/ThreadLocalPoolTest
  common/tupleschema_test
  common/undolog_test
  common/valuearray_test
  common/uniqueid_test
  execution/add_drop_table
  execution/engine_test
  execution/FragmentManagerTest
  executors/OptimizedProjectorTest
  executors/MergeReceiveExecutorTest
  expressions/expression_test
  expressions/function_test
  indexes/index_key_test
  indexes/index_scripted_test
  indexes/index_test
  indexes/CompactingHashIndexTest
  indexes/CompactingTreeMultiIndexTest
  indexes/CoveringCellIndexTest
  storage/CompactionTest
  storage/CopyOnWriteTest
  storage/DRBinaryLog_test
  storage/DRTupleStream_test
  storage/ExportTupleStream_test
  storage/PersistentTableMemStatsTest
  storage/StreamedTable_test
  storage/TempTableLimitsTest
  storage/constraint_test
  storage/filter_test
  storage/persistent_table_log_test
  storage/persistenttable_test
  storage/serialize_test
  storage/table_and_indexes_test
  storage/table_test
  storage/tabletuple_export_test
  storage/tabletuplefilter_test
  structures/CompactingMapTest
  structures/CompactingMapIndexCountTest
  structures/CompactingHashTest
  structures/CompactingPoolTest
  structures/CompactingMapBenchmark
  plannodes/WindowFunctionPlanNodeTest
  plannodes/PlanNodeFragmentTest
)

#
# Auto-generated test programs are created by calling a Java program
# in the server.
#
# First, name all the classes we want to use.
#
SET(VOLTDB_GENERATED_TEST_GENERATORS
  org.voltdb.planner.eegentests.GenerateEETests
)

SET(VOLTDB_TEST_CLASS_LIST "--build=${VOLTDB_BUILD_TYPE_LOWER}")
FOREACH (CLASS ${VOLTDB_GENERATED_TEST_GENERATORS})
  LIST(APPEND VOLTDB_TEST_CLASS_LIST --test-class=${CLASS})
ENDFOREACH()
EXECUTE_PROCESS(
    COMMAND ${VOLTDB_ROOT}/src/ee/tools/generate-ee-unit-tests.sh --voltdbroot=${VOLTDB_ROOT} ${VOLTDB_TEST_CLASS_LIST}
    OUTPUT_VARIABLE VOLTDB_GENERATED_TEST_PROGRAMS
  )
# MESSAGE( "VOLTDB_GENERATED_TEST_PROGRAMS is ${VOLTDB_GENERATED_TEST_PROGRAMS}" )

#
# The list of all test programs is the union of
# the manual tests and the generated tests.
#
SET(VOLTDB_TEST_PROGRAMS "")
LIST(APPEND VOLTDB_TEST_PROGRAMS ${VOLTDB_GENERATED_TEST_PROGRAMS})
LIST(APPEND VOLTDB_TEST_PROGRAMS ${VOLTDB_MANUAL_TEST_PROGRAMS})

ADD_LIBRARY(voltdb_test_harness
  OBJECT
  ${VOLTDB_ROOT}/tests/ee/harness)

TARGET_INCLUDE_DIRECTORIES(voltdb_test_harness
  SYSTEM PUBLIC
  ${VOLTDB_ROOT}/third_party/cpp
  PUBLIC
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)

#
# Calculate the list of directories and the list of
# tests in each directory.  The test names must be be
# unique, even ignoring directories.
#
SET ( VOLTDB_TEST_DIR_LIST "")
SET ( VOLTDB_TEST_ALLTESTS "")

FOREACH(TEST_NAME ${VOLTDB_TEST_PROGRAMS})
  # Split the name into path components.
  STRING(REGEX MATCHALL [^/]+ TEST_NAME_LIST ${TEST_NAME})
  # MESSAGE("TEST_NAME_LIST is ${TEST_NAME_LIST}")
  LIST(LENGTH TEST_NAME_LIST TEST_NAME_LIST_LENGTH)
  # MESSAGE("TEST_NAME_LIST_LENGTH is ${TEST_NAME_LIST_LENGTH}")
  # A test name will look like dir/test or else
  # generated/dir/test.  Indexing into a CMake list
  # with negative numbers get elements from the end.
  # So, the test is always at -1, the dir is always
  # at -2 and, if the generated exists, it is always
  # at -3.
  LIST(GET TEST_NAME_LIST -1 TEST)
  LIST(GET TEST_NAME_LIST -2  DIR)
  IF( TEST_NAME_LIST_LENGTH GREATER 2 )
    LIST(GET TEST_NAME_LIST -3 GENERATED)
  ELSE()
    SET(GENERATED ".")
  ENDIF()
  # MESSAGE("TEST_NAME ${TEST_NAME}: GENERATED ${GENERATED}, DIR ${DIR}, TEST ${TEST}")
  # Find if this test has been seen before.
  # If so it will be on the ALLTESTS list.
  LIST(FIND VOLTDB_TEST_ALLTESTS ${TEST} FIND_IDX)
  IF (NOT ${FIND_IDX} LESS 0)
    MESSAGE(FATAL_ERROR "Duplicate test name ${TEST_NAME}")
  ENDIF()
  LIST(APPEND VOLTDB_TEST_ALLTESTS ${TEST})
  # Find if this test directory has been
  # seen before.  If not, then append the
  # name to the test_dir_list.
  LIST(FIND VOLTDB_TEST_DIR_LIST ${DIR} FIND_IDX)
  IF (${FIND_IDX} LESS 0)
    LIST(APPEND VOLTDB_TEST_DIR_LIST ${DIR})
  ENDIF()
  SET(VOLTDB_TESTDIR_${TEST} ${DIR})
  SET(VOLTDB_TESTGEN_${TEST} ${GENERATED})
ENDFOREACH()
########################################################################
#
# Create run-all-tests and build-all-tests.  We will make
# build-all-tests depend on building each individual test.
# So there's no command necessary.  However, we need to make
# the test directory first.
#
########################################################################
ADD_CUSTOM_TARGET(build-all-tests)
ADD_CUSTOM_TARGET(run-all-tests
  COMMAND /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -j ${VOLTDB_CORE_COUNT})
ADD_DEPENDENCIES(run-all-tests build-all-tests)

########################################################################
#
# For each test directory, create a target.  We will
# make this target depend on the test executable later
# on.
#
########################################################################
FOREACH (TEST_DIR ${VOLTDB_TEST_DIR_LIST})
  # We assign each test the label of its test directory.
  # So running all tests is just running all tests with
  # the given label.
  ADD_CUSTOM_TARGET(run-${TEST_DIR}
    COMMAND /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -j ${VOLTDB_CORE_COUNT} -L ${TEST_DIR})
  ADD_CUSTOM_TARGET(build-${TEST_DIR})
  ADD_CUSTOM_COMMAND(TARGET build-${TEST_DIR}
    PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory cpptests/${TEST_DIR}
    )
  ADD_DEPENDENCIES(build-all-tests build-${TEST_DIR})
ENDFOREACH()

########################################################################
#
# For each test, on the VOLTDB_TEST_ALLTESTS list, generate
# targets, commands and dependences.
#
########################################################################
FOREACH (TEST_NAME ${VOLTDB_TEST_ALLTESTS})
  SET(TEST_DIR ${VOLTDB_TESTDIR_${TEST_NAME}})
  SET(TEST_GEN ${VOLTDB_TESTGEN_${TEST_NAME}})
  # MESSAGE("TEST_DIR: ${TEST_DIR}, TEST_NAME: ${TEST_NAME}, TEST_GEN: ${TEST_GEN}")
  #
  # Define the test executable, and set the build-all-tests
  # depends on the test.  Remember the necessary include
  # directories and link libraries.
  #
  ADD_EXECUTABLE(${TEST_NAME}
      $<TARGET_OBJECTS:voltdb_test_harness>
      $<TARGET_OBJECTS:voltdbobjs>
      $<TARGET_OBJECTS:third_party_objs>
      ${VOLTDB_ROOT}/tests/ee/${TEST_GEN}/${TEST_DIR}/${TEST_NAME}.cpp)
  ADD_CUSTOM_COMMAND(TARGET ${TEST_NAME}
    PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory cpptests/${TEST_DIR}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/tests/test_working_dir
    )
  #
  # Calculate the actual test command.
  #
  SET(TARGET_EXE_CMD ${CMAKE_BINARY_DIR}/cpptests/${TEST_DIR}/${TEST_NAME})
  ADD_CUSTOM_TARGET(build-${TEST_NAME}
    DEPENDS ${TEST_NAME})
  ADD_DEPENDENCIES(build-${TEST_DIR} ${TEST_NAME})
  SET_TARGET_PROPERTIES(${TEST_NAME}
    PROPERTIES
    OUTPUT_NAME ${TARGET_EXE_CMD}
    EXCLUDE_FROM_ALL TRUE)
  ADD_DEPENDENCIES(${TEST_NAME}
    pcre2 crypto s2geo ${VOLTDB_LIBNAME})
  TARGET_INCLUDE_DIRECTORIES(${TEST_NAME}
    SYSTEM PUBLIC
    ${VOLTDB_ROOT}/third_party/cpp
    PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}/3pty-install/include
    ${VOLTDB_ROOT}/tests/ee)
  TARGET_LINK_LIBRARIES(${TEST_NAME}
    ${VOLTDB_LINK_FLAGS}
    -L${CMAKE_BINARY_DIR}/3pty-install/lib
    -lpcre2-8 -ls2geo -lcrypto
    -ldl)
  #
  # We expect all the memleak tests to fail, except for
  # the test no_losses.  It's possible, but not obvious,
  # how to do this with a regular expression property
  # on the test, but this seems more straightforward.
  #
  SET(WILL_FAIL FALSE)
  LIST(FIND VOLTDB_FAILING_VALGRIND_DIRS ${TEST_DIR} VG_DIR_FAIL)
  LIST(FIND VOLTDB_PASSING_VALGRIND_TESTS ${TEST_NAME} VG_TEST_PASS)
  IF (${IS_VALGRIND_BUILD} AND (NOT (${VG_DIR_FAIL} LESS 0)))
    IF (${VG_TEST_PASS} LESS 0)
      SET (WILL_FAIL TRUE)
    ENDIF()
  ENDIF()
  # If we are running a valgrind test, then calculate the
  # valgrind command.  The output variable, VALGRIND_EXE_CMD,
  # may be the same as TARGET_EXE or else it may be a call
  # to valgrind.
  VALGRIND_COMMAND(${TEST_DIR} ${TEST_NAME} ${TARGET_EXE_CMD} ${WILL_FAIL} VALGRIND_EXE_CMD IS_VALGRIND_TEST)
  # Some tests need to run python.  This needs to be
  # second because we want to run valgrind on the actual
  # test executable, TARGET_EXE_CMD, and not on python.
  PYTHON_COMMAND(${TEST_DIR} ${TEST_NAME} "${VALGRIND_EXE_CMD}" CTEST_EXE_CMD OUTPUT_IS_PYTHON_TEST)
  # So, "make ${TEST_NAME}"" builds the test and
  # "make run-${TEST_NAME}" runs the single test.
  # MESSAGE("Test run-${TEST_NAME} has command ${CTEST_EXE_CMD}.")
  ADD_TEST(NAME ${TEST_NAME}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests/test_working_dir
    COMMAND ${CTEST_EXE_CMD})
  ADD_DEPENDENCIES(run-${TEST_DIR} ${TEST_NAME})
  ADD_CUSTOM_TARGET(run-${TEST_NAME}
    DEPENDS ${TEST_NAME}
    COMMAND /usr/bin/env /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -j ${VOLTDB_CORE_COUNT} -R ${TEST_NAME})
  # Some tests are expected to fail.  Also, tag the
  # test with its label.
  SET_TESTS_PROPERTIES(${TEST_NAME}
    PROPERTIES
    WILL_FAIL ${WILL_FAIL}
    LABELS ${TEST_DIR})
ENDFOREACH()
