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
# LICENSE file.
#   1.) mkdir -p obj/${BUILD_TYPE}
#       where ${BUILD_TYPE} is memcheck, release or debug.
#   2.) cd obj/${BUILD_TYPE}
#   3.) cmake -DVOLTDB_BUILD_TYPE=${BUILD_TYPE} <VOLTDB_ROOT>
#       where <VOLTDB_ROOT> is the pathname of the root of the voltdb
#       source directory, typically where you found this file.
#       There are other arguments which could be added.  See build.xml.
#       CMake looks at the build environment and creates makefiles
#       aplenty.  This will configure the binary directories
#       ${CMAKE_BINARY_DIR}/ee for VoltDB_EE and
#       ${CMAKE_BINARY_DIR}/tests for VoltDB_Tests.
#   4.) make -j <N> -k install
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
#       Note that in memcheck builds we run under valgrind.  The test
#       system knows how to do this. Note also that no output is shown
#       from any test unless the test fails.  If you run the test
#       executable you can see the test output.
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
  MESSAGE (FATAL_ERROR "VOLTDB_BUILD_TYPE is not defined.")
ENDIF ()

BANNER("Configuring the VoltDB Execution Engine."
       "CMAKE_SOURCE_DIRECTORY: ${CMAKE_SOURCE_DIRECTORY}"
       "CMAKE_BINARY_DIRECTORY: ${CMAKE_BINARY_DIRECTORY}"
       "CMAKE_GENERATOR: ${CMAKE_GENERATOR}"
       "${PROJECT_NAME}: PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}"
       "${PROJECT_NAME}: PROJECT_BINARY_DIRECTORY is ${${PROJECT_NAME}_BINARY_DIR}"
       "VOLTDB_BUILD_TYPE: ${VOLTDB_BUILD_TYPE}"
   )

########################################################################
#
# Set some variables useful to all the sub projects.
#   VOLTDB_VERSION
#   VOLT_LOG_LEVEL
#
########################################################################
EXECUTE_PROCESS(COMMAND cat ${CMAKE_SOURCE_DIR}/version.txt OUTPUT_VARIABLE VOLTDB_VERSION)
STRING(STRIP ${VOLTDB_VERSION} VOLTDB_VERSION)
SET (VOLT_LOG_LEVEL 500)

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
