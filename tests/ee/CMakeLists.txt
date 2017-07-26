# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
########################################################################
#
# A CMake build configuration for VoltDB EE Unit Tests.
#
# There are two kinds of EE Unit Tests.  One kind, called
# a manual test, is written as usual, by creating a C++
# text file from scratch.  The other is written by one
# of several Java classes which hold a specification of
# the input and output tables, the plan of a SQL statement,
# the catalog string of some DDL and a specification of
# the expected output.
#
########################################################################

CMAKE_MINIMUM_REQUIRED (VERSION 2.8.12 FATAL_ERROR)
PROJECT(VoltDB_Tests)
IF ( (CMAKE_SYSTEM_NAME STREQUAL "Darwin") AND ( (CMAKE_VERSION VERSION_GREATER 3.0.0) OR (CMAKE_VERSION VERSION_EQUAL 3.0.0) ) )
    # Newer versions of CMake than we use set this
    # policy on the mac.  There there is a way to set the
    # rpath which is different from Linux.  We just ignore
    # it, but we need to use the old behavior on the mac.
    CMAKE_POLICY(SET CMP0042 OLD)
ENDIF()

BANNER("Configuring the VoltDB Execution Engine Unit Tests.")
########################################################################
#
# Here we have all the names of tests which are not auto-generated
# and which are expected to pass.  Mostly if you add a new test and
# you expect to it to succeed, just add it to this list.  If it's
# expected to fail add it to VOLTDB_MANUAL_FAILING_TEST_PROGRAMS.
# If it's expected to fail in valgrind but succeed otherwise,
# add it to VOLTDB_MANUAL_VALGRIND_FAILING_TEST_PROGRAMS.
#
# They have the form directory/test_name.  The test names must all be
# globally unique.  We test for this later on.
#
########################################################################
SET (VOLTDB_MANUAL_SUCCEEDING_TEST_PROGRAMS
  harness_test/harness_tester
  catalog/catalog_test
  logging/logging_test
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
# These are like the earlier tests, but they
# are expected to fail when run under valgrind.
#
SET(VOLTDB_MANUAL_VALGRIND_FAILING_TEST_PROGRAMS
  memleaktests/definite_losses
  memleaktests/indirect_losses
  memleaktests/still_reachable_losses
  memleaktests/possible_losses
  memleaktests/rw_deleted
)
#
# These are expected to fail.  There are none of
# these now, but the harness_tester test has one that
# should fit into this category.
#
SET (VOLTDB_MANUAL_FAILING_TEST_PROGRAMS
  )
#
# Auto-generated test programs are created by calling a Java program
# in the server.  The java class we run is named here.  We may
# name more than one class.
#
# First, name all the classes we want to use.
#
SET(VOLTDB_GENERATED_TEST_GENERATORS
  org.voltdb.planner.eegentests.GenerateEETests
)

SET(VOLTDB_TEST_BUILD_TYPE "--build-type=${VOLTDB_BUILD_TYPE_LOWER}")
FOREACH (CLASS ${VOLTDB_GENERATED_TEST_GENERATORS})
  LIST(APPEND VOLTDB_TEST_CLASS_LIST --test-class=${CLASS})
ENDFOREACH()
EXECUTE_PROCESS(
    COMMAND ${CMAKE_SOURCE_DIR}/tools/generate-ee-unit-tests.sh --voltdbroot=${CMAKE_SOURCE_DIR} ${VOLTDB_TEST_BUILD_TYPE} ${VOLTDB_TEST_CLASS_LIST}
    OUTPUT_VARIABLE VOLTDB_GENERATED_TEST_PROGRAMS
  )
STRING(STRIP "${VOLTDB_GENERATED_TEST_PROGRAMS}" VOLTDB_GENERATED_TEST_PROGRAMS)
MESSAGE( "VOLTDB_GENERATED_TEST_PROGRAMS is ${VOLTDB_GENERATED_TEST_PROGRAMS}" )

#
# The list of all test programs is the union of
# the manual tests and the generated tests.
#
SET(VOLTDB_TEST_ALLTESTS "")
SET(VOLTDB_TEST_DIR_LIST "")

#
# The calls to ADD_TEST_PROGRAM adds the test names
# to VOLTDB_TEST_ALLTEST and the test directories
# to VOLTDB_TEST_DIR_LIST.  We also set variables
# for each test telling if it's generated and if
# it's expected to fail.
#
FOREACH(PROG ${VOLTDB_MANUAL_SUCCEEDING_TEST_PROGRAMS})
  # FALSE means we don't expect a failure.
  ADD_TEST_PROGRAM(${PROG} FALSE VOLTDB_TEST_PROGRAMS)
ENDFOREACH()
FOREACH(PROG ${VOLTDB_MANUAL_FAILING_TEST_PROGRAMS})
  # FALSE means we don't expect a failure.
  ADD_TEST_PROGRAM(${PROG} TRUE VOLTDB_TEST_PROGRAMS)
ENDFOREACH()
FOREACH(PROG ${VOLTDB_MANUAL_VALGRIND_FAILING_TEST_PROGRAMS})
  # If IS_VALGRIND_BUILD is TRUE then we
  # expect a failure.  Otherwise we don't.
  ADD_TEST_PROGRAM(${PROG} ${IS_VALGRIND_BUILD} VOLTDB_TEST_PROGRAMS)
ENDFOREACH()
FOREACH(PROG ${VOLTDB_GENERATED_TEST_PROGRAMS})
  # FALSE means we don't expect a failure.
  ADD_TEST_PROGRAM(${PROG} FALSE VOLTDB_TEST_PROGRAMS)
ENDFOREACH()

ADD_LIBRARY(voltdb_test_harness
  OBJECT
  ${CMAKE_SOURCE_DIR}/tests/ee/harness)

MESSAGE("VoltDB_Tests: PROJECT_NAME Is ${PROJECT_NAME}")
MESSAGE("VoltDB_Tests: PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}")
TARGET_INCLUDE_DIRECTORIES(voltdb_test_harness
  SYSTEM
  PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp)
TARGET_INCLUDE_DIRECTORIES(voltdb_test_harness
  PUBLIC
  ${${PROJECT_NAME}_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/src/ee
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include)

########################################################################
#
# Calculate the list of directories and the list of
# tests in each directory.  The test names must be be
# unique, even ignoring directories.
#
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
ADD_CUSTOM_TARGET(build-generated-tests)
ADD_CUSTOM_TARGET(run-generated-tests
  COMMAND /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -L generated -j ${VOLTDB_CORE_COUNT})
ADD_CUSTOM_TARGET(build-manual-tests)
ADD_CUSTOM_TARGET(run-manual-tests
  COMMAND /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -L manual -j ${VOLTDB_CORE_COUNT})
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

MESSAGE("VOLTDB_TEST_ALLTESTS IS ${VOLTDB_TEST_ALLTESTS}")
FOREACH (TEST_NAME ${VOLTDB_TEST_ALLTESTS})
  DEFINE_TEST(${TEST_NAME})
ENDFOREACH()
