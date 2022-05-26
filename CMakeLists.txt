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
# A CMake build configuration for VoltDB.  The commands for the ant
# targets in build.xml call tools/build_cmake.py, which calls
# cmake and make here, so you don't really need to know how to use this.
# But for special cases this may be useful.
#
# There are three projects here.  One is the root project, called
# VoltDB.  This is a container for the other two, and defines some
# variables common to the two.  The others are:
#   1.) VoltDB_EE
#       This is the project which is responsible for building the
#       VoltDB Execution Engine's shared library.  It's also
#       responsible for building the voltdbipc program, which can
#       be used to debug the EE standalone.
#   2.) VoltDB_Tests
#       This project is responsible for building the tests.  Some
#       tests are manually written, and some are auto generated
#       from descriptions in the server's java code.  This requires
#       that the Java code be compiled first and that the jars
#       be packaged up, and this requires the shared library from
#       the VoltDB_EE package.  So this is a separate CMake project.
#
# The two output artifacts are:
#   1.) The jni library, which will be in
#           obj/${BUILD}/ee/nativelibs/libvoltdb-VERSION.so
#       for Linux and
#           obj/${BUILD}/ee/nativelibs/libvoltdb-VERSION.dylib
#       for Darwin.
#   2.) The voltdbipcrun program, which is used for memory leak detection,
#       and to debug this EE.  This will be found in
#           obj/${BUILD}/ee/prod/voltdbipcrun
#       on both Linux and Darwin.
#
# The main way to build this, and the procedure the ant build uses,
# is to run "tools/build_cmake.py".  Run this command with --help
# for details.
#
# To use this standalone, in the root of the voltdb hierarchy, run this
# procedure. All files are relative to the VoltDB source tree root.  This
# is the directory which contains src, test, voltdb and the VoltDB
# LICENSE file.  Names in curly braces, like ${THIS}, are names of
# things in your build, like the build type or the build directory.
#   1.) mkdir -p obj/${BUILD_TYPE}
#       where ${BUILD_TYPE} is memcheck, release or debug.
#   2.) cd obj/${BUILD_TYPE}
#   3.) cmake -DVOLTDB_BUILD_TYPE=${BUILD_TYPE} ${VOLTDB_ROOT}
#       where ${VOLTDB_ROOT} is the pathname of the root of the voltdb
#       source directory, typically where you found this file.
#       There are other arguments which could be added.  See build.xml.
#       CMake looks at the build environment and creates makefiles
#       aplenty.  This will configure the binary directories
#       ${CMAKE_BINARY_DIR}/ee for VoltDB_EE and
#       ${CMAKE_BINARY_DIR}/tests for VoltDB_Tests.
#   4.) cd obj/${BUILD}; make -j <N> -k install
#       where, as usual, <N> is number telling make how much parallelism
#       to use.  Values between 1 and 8 are typical.  Higher values
#       may produce faster builds, or they may just gum things up.
#       If you use N=50 you will have to reboot your computer.  Really.
#   5.) If this succeeds, then the build artifacts will be placed in
#       obj/${BUILD}/nativelibs and obj/${BUILD}/prod.  The tests
#       are not built at this point.
#   6.) There are several ways to build and run tests.  Tests are grouped
#       into directories.  You can build and run individual tests, all tests
#       in a directory and all tests.
#       a.) To run the tests run "make -j <N> -k run-all-tests".
#       b.) To run a particular test run "make run-TEST".
#       c.) To run all tests is a director run "make run-TESTDIR"
#       d.) To build but not run tests, use the commands above, but
#           use build- rather than run-.
#       e.) There are labels for sets of tests.
#           i.) The label manual means tests which have been created
#               manually.  Most tests are manual tests.
#           ii.) The label generated means tests which have been generated.
#           iii.) Each test folder names a label.  So common, storage,
#               catalog and so forth are labels.
#       f.) To run all tests labelled L use "make run-L-tests".  For example,
#           "make run-catalog-tests" runs the catalog tests, or
#           "make run-generated-tests" runs the generated tests.
#       Note that in memcheck builds we run under valgrind.  The test
#       system knows how to do this. Note also that no output is shown
#       from any test unless the test fails.  If you run the test
#       executable you can see the test output.  There is a bug in
#       the CMake program ctest which precludes listing all output
#       easily.
#
# Just to review the way CMake works:
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
#
# There's the potential for making Eclipse or XCode projects here.
# Also, there's the potential for substituting ninja for make, which
# would improve EE build times.  More experience is needed.
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
# version of at least 2.8.11.  It's possible that older versions of
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
# You probably need python 2.7.  We don't know if python 2.6 or earlier
# works, or if python 3 works.
#
########################################################################
# There are a few rules which may make things be more organized,
# both here and in the subprojects in src/ee and tests/ee.
#
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
#      ${CMAKE_BINARY_DIR}/3pty_install
#                           All third party artifacts on which which
#                           voltdb depends go here, in lib or include.
#                           There are some other subfolders created
#                           by the third party install procedures,
#                           but we don't care about anything but the
#                           static libraries and includes.
########################################################################

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

CMAKE_MINIMUM_REQUIRED (VERSION 2.8.11 FATAL_ERROR)
PROJECT(VoltDB)
INCLUDE(ExternalProject)
IF ( (CMAKE_SYSTEM_NAME STREQUAL "Darwin") AND
    ( (CMAKE_VERSION VERSION_GREATER 3.0.0)
      OR (CMAKE_VERSION VERSION_EQUAL 3.0.0) ) )
    # Newer versions of CMake than we use set this
    # policy on the mac.  There there is a way to set the
    # rpath which is different from Linux.  We just ignore
    # it, but we need to use the old behavior on the mac.
    CMAKE_POLICY(SET CMP0042 OLD)
ENDIF()

########################################################################
#
# Some functions to make things look simpler.  These
# are needed in both the src/ee and tests/ee projects.
#
########################################################################
INCLUDE(tools/VoltDB.cmake)

########################################################################
#
# Sanity check the configuration.
#
########################################################################
IF (NOT DEFINED VOLTDB_BUILD_TYPE)
  STRING(TOLOWER ${CMAKE_BUILD_TYPE} VOLTDB_BUILD_TYPE)
ENDIF()
IF (NOT (${VOLTDB_BUILD_TYPE} STREQUAL "debug")
    AND NOT (${VOLTDB_BUILD_TYPE} STREQUAL "release")
    AND NOT (${VOLTDB_BUILD_TYPE} STREQUAL "memcheck"))
    MESSAGE(FATAL_ERROR "Unknown VoltDB build type ${VOLTDB_BUILD_TYPE}")
ENDIF()

########################################################################
#
# Set some variables useful to all the sub projects.
#   VOLTDB_VERSION
#   VOLT_LOG_LEVEL
#
########################################################################
EXECUTE_PROCESS(COMMAND cat ${CMAKE_SOURCE_DIR}/version.txt OUTPUT_VARIABLE VOLTDB_VERSION)
STRING(STRIP ${VOLTDB_VERSION} VOLTDB_VERSION)
IF (NOT DEFINED VOLT_LOG_LEVEL)
    SET (VOLT_LOG_LEVEL 500)
ENDIF()

BANNER("Configuring the VoltDB Execution Engine."
       "CMAKE_SOURCE_DIRECTORY: ${CMAKE_SOURCE_DIR}"
       "CMAKE_BINARY_DIRECTORY: ${CMAKE_BINARY_DIR}"
       "CMAKE_GENERATOR:        ${CMAKE_GENERATOR}"
       "${PROJECT_NAME}:        PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}"
       "${PROJECT_NAME}:        PROJECT_BINARY_DIRECTORY is ${${PROJECT_NAME}_BINARY_DIR}"
       "VOLTDB_BUILD_TYPE:      ${VOLTDB_BUILD_TYPE}"
       "VOLTDB_CORE_COUNT:      ${VOLTDB_CORE_COUNT}"
       "VOLT_LOG_LEVEL:         ${VOLT_LOG_LEVEL}"
       "VOLT_POOL_CHECKING:     ${VOLT_POOL_CHECKING}"
       "VOLT_TRACE_ALLOCATIONS: ${VOLT_TRACE_ALLOCATIONS}"
       "VOLT_TIMER_ENABLED:     ${VOLT_TIMER_ENABLED}"
   )

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
# Encapsulate the compilation flags and definitions in one place.
#
########################################################################
INCLUDE(tools/VoltDBCompilation.cmake)

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

OPTION(VOLT_POOL_CHECKING "Turn on extra memory pool checking" OFF)

SET (VOLTDB_TARBALLS ${CMAKE_SOURCE_DIR}/third_party/cpp/tarballs)
SET (VOLTDB_3PTY_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/3pty-install)
SET (VOLTDB_3PTY_BUILD_PREFIX ${${PROJECT_NAME}_BINARY_DIR}/3pty-obj)
########################################################################
#
# Go and build the openssl library.  We only use this
# for the arbitrary precision arithmetic code, which
# S2GEO uses.  We have to untar it and build it, which
# we do at build time.
#
########################################################################
SET (VOLTDB_OPENSSL_VERSION 1.0.2t)
SET (VOLTDB_OPENSSL_NAME openssl-${VOLTDB_OPENSSL_VERSION})
# We unpack the tarball in the binary directory. Same with pcre2 above.
SET (VOLTDB_OPENSSL_TARBALL ${VOLTDB_TARBALLS}/${VOLTDB_OPENSSL_NAME}.tar.gz)
# VOLTDB_3PTY_INSTALL_PREFIX:      voltdb/obj/${VOLTDB_BUILD_TYPE}/ee/3pty-install
# VOLTDB_3PTY_BUILD_PREFIX:        voltdb/obj/${VOLTDB_BUILD_TYPE}/ee/3pty-obj
# VOLTDB_OPENSSL_BUILD_PREFIX:     /openssl
# VOLTDB_OPENSSL_SRC:              /src/openssl-1.0.2t/ ... unpacked source
# VOLTDB_OPENSSL_OBJ:              /src/openssl-1.0.2t/ ... generated binaries
# Binary and source are the same for openssl.  There
# are not obvious ways to do out-of-tree builds.
SET (VOLTDB_OPENSSL_BUILD_PREFIX ${VOLTDB_3PTY_BUILD_PREFIX}/openssl)
SET (VOLTDB_OPENSSL_SRC ${VOLTDB_OPENSSL_BUILD_PREFIX}/src/${VOLTDB_OPENSSL_NAME})
SET (VOLTDB_OPENSSL_OBJ ${VOLTDB_OPENSSL_SRC})

BANNER("Describing OPENSSL"
       "  SOURCE at ${VOLTDB_OPENSSL_SRC}"
       "  BINARY at ${VOLTDB_OPENSSL_OBJ}"
       "  TARBALL at ${VOLTDB_OPENSSL_TARBALL}"
       "  CONFIGURE_COMMAND: ${VOLTDB_OPENSSL_SRC}/Configure --prefix=${VOLTDB_3PTY_INSTALL_PREFIX} ${VOLTDB_OPENSSL_TOKEN}"
   )


ExternalProject_Add(crypto
  SOURCE_DIR ${VOLTDB_OPENSSL_SRC}
  INSTALL_DIR ${VOLTDB_3PTY_INSTALL_PREFIX}
  DOWNLOAD_DIR ${VOLTDB_OPENSSL_BUILD_PREFIX}/src
  URL ${VOLTDB_OPENSSL_TARBALL}
  CONFIGURE_COMMAND ${VOLTDB_OPENSSL_SRC}/Configure --prefix=${VOLTDB_3PTY_INSTALL_PREFIX} ${VOLTDB_OPENSSL_TOKEN}
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND make install_sw
  )
ExternalProject_get_property(crypto INSTALL_DIR)
SET(VOLTDB_CRYPTO_INSTALL_DIR ${INSTALL_DIR})

########################################################################
#
# Go and build the pcre2 library.
# We have to untar it and configure it.  We will
# do this at build time.  There is supposed to be a way
# to do this with a CMake build, but we don't use it.
#
########################################################################
SET (VOLTDB_PCRE2_VERSION 10.33)
SET (VOLTDB_PCRE2_NAME pcre2-${VOLTDB_PCRE2_VERSION})
# We unpack the tarball in the binary directory.  It saves us
# checking it all in to github.  That's why this is
# ${PROJECT_NAME}_BINARY_DIR.
SET (VOLTDB_PCRE2_TARBALL ${VOLTDB_TARBALLS}/${VOLTDB_PCRE2_NAME}.tar.gz)
#
# VOLTDB_3PTY_INSTALL_PREFIX:     voltdb/obj/${VOLTDB_BUILD_TYPE}/ee/3pty-install
# VOLTDB_3PTY_BUILD_PREFIX:       voltdb/obj/${VOLTDB_BUILD_TYPE}/ee/3pty-obj
# VOLTDB_PCRE2_BUILD_PREFIX:      /pcre2
# VOLTDB_PCRE2_SRC:               /src/pcre2-10.33/ ... unpacked source
# VOLTDB_PCRE2_OBJ:               /obj/pcre2-10.33/ ... generated binaries
SET (VOLTDB_PCRE2_BUILD_PREFIX ${VOLTDB_3PTY_BUILD_PREFIX}/pcre2)
SET (VOLTDB_PCRE2_SRC ${VOLTDB_PCRE2_BUILD_PREFIX}/src/${VOLTDB_PCRE2_NAME})
SET (VOLTDB_PCRE2_OBJ ${VOLTDB_PCRE2_BUILD_PREFIX}/obj/${VOLTDB_PCRE2_NAME})

BANNER("Describing PCRE2"
       "  SOURCE at ${VOLTDB_PCRE2_SRC}"
       "  BINARY at ${VOLTDB_PCRE2_OBJ}"
       "  TARBALL at ${VOLTDB_PCRE2_TARBALL}"
       "  CONFIGURE_COMMAND: ./configure --disable-shared --with-pic --prefix=${VOLTDB_3PTY_INSTALL_PREFIX}"
   )
ExternalProject_Add(pcre2
  SOURCE_DIR ${VOLTDB_PCRE2_SRC}
  BINARY_DIR ${VOLTDB_PCRE2_OBJ}
  INSTALL_DIR ${VOLTDB_3PTY_INSTALL_PREFIX}
  DOWNLOAD_DIR ${VOLTDB_PCRE2_BUILD_PREFIX}/src
  URL ${VOLTDB_PCRE2_TARBALL}
  CONFIGURE_COMMAND ${VOLTDB_PCRE2_SRC}/configure --disable-shared --with-pic --prefix=${VOLTDB_3PTY_INSTALL_PREFIX}
  )

########################################################################
#
# Go and build the S2GEO library.
# This is built with CMake.  We use a somewhat customized version
# of the library.
#
########################################################################
SET (VOLTDB_S2GEO_SRC "${CMAKE_SOURCE_DIR}/third_party/cpp/google-s2-geometry")
SET (VOLTDB_S2GEO_OBJ ${${PROJECT_NAME}_BINARY_DIR}/3pty-obj/google-s2-geometry)
SET (VOLTDB_S2GEO_CMAKE_CONFIG
  -DCXX_VERSION_FLAG=${CXX_VERSION_FLAG}
  -DVOLTDB_THIRD_PARTY_CPP_DIR=${CMAKE_SOURCE_DIR}/third_party/cpp
  -DCMAKE_INSTALL_PREFIX=${VOLTDB_3PTY_INSTALL_PREFIX}/
  -DCMAKE_BUILD_TYPE=${VOLTDB_BUILD_TYPE})
SET (VOLTDB_S2GEO_PREFIX ${VOLTDB_3PTY_BUILD_PREFIX}/google-s2-geometry)

BANNER("Describing S2GEO"
       "  PREFIX at ${VOLTDB_S2GEO_PREFIX}"
       "  SOURCE at ${VOLTDB_S2GEO_SRC}"
       "  BINARY at ${VOLTDB_S2GEO_OBJ}"
       "  CMAKE_ARGS: ${VOLTDB_S2GEO_CMAKE_CONFIG}"
       "  CMAKE_GENERATOR: ${CMAKE_GENERATOR}"
   )
ExternalProject_Add(s2geo
  PREFIX ${VOLTDB_S2GEO_PREFIX}  # Not sure this is needed.
  SOURCE_DIR ${VOLTDB_S2GEO_SRC}
  BINARY_DIR ${VOLTDB_S2GEO_OBJ}
  CMAKE_ARGS ${VOLTDB_S2GEO_CMAKE_CONFIG}
  CMAKE_GENERATOR ${CMAKE_GENERATOR}
  )
ADD_DEPENDENCIES(s2geo crypto)
########################################################################
#
# Configure the ee files.
#
########################################################################
ADD_SUBDIRECTORY(src/ee ee)

########################################################################
#
# Configure the tests.
#
########################################################################
ENABLE_TESTING()
ADD_SUBDIRECTORY(tests/ee tests)

