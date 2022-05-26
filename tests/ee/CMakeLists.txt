# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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
# the expected output.  These latter tests are called generated
# tests.
#
########################################################################
CMAKE_MINIMUM_REQUIRED (VERSION 2.8.11 FATAL_ERROR)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

PROJECT(VoltDB_Tests)
IF ( (CMAKE_SYSTEM_NAME STREQUAL "Darwin") AND ( (CMAKE_VERSION VERSION_GREATER 3.0.0) OR (CMAKE_VERSION VERSION_EQUAL 3.0.0) ) )
    # Newer versions of CMake than we use set this
    # policy on the mac.  There there is a way to set the
    # rpath which is different from Linux.  We just ignore
    # it, but we need to use the old behavior on the mac.
    CMAKE_POLICY(SET CMP0042 OLD)
ENDIF()

COMPUTE_CORE_COUNT(VOLTDB_CORE_COUNT)
# MESSAGE("VOLTDB_CORE_COUNT IS ${VOLTDB_CORE_COUNT}")

BANNER("Configuring the VoltDB Execution Engine Unit Tests."
       "CMAKE_SOURCE_DIRECTORY: ${CMAKE_SOURCE_DIR}"
       "CMAKE_BINARY_DIRECTORY: ${CMAKE_BINARY_DIR}"
       "${PROJECT_NAME}:        PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}"
       "${PROJECT_NAME}:        PROJECT_BINARY_DIRECTORY is ${${PROJECT_NAME}_BINARY_DIR}"
       "VOLTDB_BUILD_TYPE:      ${VOLTDB_BUILD_TYPE}"
       "VOLTDB_CORE_COUNT:      ${VOLTDB_CORE_COUNT}"
       "VOLT_LOG_LEVEL:         ${VOLT_LOG_LEVEL}"
       "VOLT_POOL_CHECKING:     ${VOLT_POOL_CHECKING}"
   )

########################################################################
#
# Here we have all the names of tests which are not generated
# and which are expected to pass.  Mostly if you add a new test and
# you expect to it to succeed, just add it to this list.  If it's
# expected to fail add it to VOLTDB_MANUAL_FAILING_TEST_PROGRAMS.
# If it's expected to fail in valgrind but succeed otherwise,
# add it to VOLTDB_MANUAL_VALGRIND_FAILING_TEST_PROGRAMS.
#
# Note that these are the names of the test executables and not
# the test source files.  The source files must have extension .cpp
# and be found in the given directory in voltdb/tests/ee.  This
# makes some of the test processing easier.
#
# They have the form directory/test_name.  The test names must all be
# globally unique.  We test for this later on.
#
# Keeping this list in alphabetical order helps find things.
# Order is not really important, though.  The ones not in
# alphabetical order are there to make them run first.
#
########################################################################
SET (VOLTDB_MANUAL_SUCCEEDING_TEST_PROGRAMS
  # These two need to run first, since they take
  # the longest to run.
  storage/LargeTempTableSortTest
  storage/DRBinaryLog_test
  catalog/catalog_test
  catalog/ExportTupleStreamTest
  common/debuglog_test
  common/elastic_hashinator_test
  common/nvalue_test
  common/LargeTempTableBlockIdTest
  common/PerFragmentStatsTest
  common/PoolCheckingTest
  common/pool_test
  common/serializeio_test
  common/tabletuple_test
  common/ThreadLocalPoolTest
  common/tupleschema_test
  common/undolog_test
  common/uniqueid_test
  common/valuearray_test
  execution/add_drop_table
  execution/engine_test
  execution/ExecutorVectorTest
  execution/FragmentManagerTest
  executors/CommonTableExpressionTest
  executors/MergeReceiveExecutorTest
  executors/OptimizedProjectorTest
  expressions/expression_test
  expressions/function_test
  indexes/CompactingHashIndexTest
  indexes/CompactingTreeMultiIndexTest
  indexes/CoveringCellIndexTest
  indexes/index_key_test
  indexes/index_scripted_test
  indexes/index_test
  harness_test/harness_tester
  topics/TopicsTableFactoryTest
  topics/GroupStoreTest
  topics/encode/EncoderTest
  topics/encode/AvroEncoderTest
  topics/encode/CsvEncoderTest
  topics/orm/GroupOrmTest
  logging/logging_test
  memleaktests/no_losses
  plannodes/PlanNodeFragmentTest
  plannodes/PlanNodeUtilTest
  plannodes/WindowFunctionPlanNodeTest
  storage/CompactionTest
  storage/constraint_test
  storage/CopyOnWriteTest
  storage/DRTupleStream_test
  storage/ExportTupleStream_test
  storage/TopicTupleStreamTest
  storage/filter_test
  storage/LargeTempTableBlockTest
  storage/LargeTempTableTest
  storage/persistent_table_log_test
  storage/PersistentTableMemStatsTest
  storage/persistenttable_test
  storage/serialize_test
  storage/StreamedTable_test
  storage/table_and_indexes_test
  storage/table_test
  storage/tabletuple_export_test
  storage/tabletuplefilter_test
  storage/TempTableLimitsTest
  storage/TableTupleAllocatorTest
  structures/CompactingHashTest
  structures/CompactingMapBenchmark
  structures/CompactingMapIndexCountTest
  structures/CompactingMapTest
  structures/CompactingPoolTest
)

#
# These are like the earlier tests, but they
# are expected to fail when run under valgrind.
# We use these to test that we are finding
# valgrind errors.
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
# These are tests which are not run under valgrind
# but are run when not in a valgrind build.  There
# are none of them now, but there used to be and
# may be in the future.
SET (VOLTDB_NOVALGRIND_TEST_PROGRAMS
)
#
# Generated test programs are created by calling a Java program
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
#
# Generate the tests.
#
EXECUTE_PROCESS(
    COMMAND ${CMAKE_SOURCE_DIR}/tools/generate-ee-unit-tests.sh --voltdbroot=${CMAKE_SOURCE_DIR} ${VOLTDB_TEST_BUILD_TYPE} ${VOLTDB_TEST_CLASS_LIST}
)
#
# The previous command left the test names in
# This file.  Each line is a single file name.  We
# need to do some CMake magic to convert this to a
# CMake list.
#
FILE(READ ${CMAKE_SOURCE_DIR}/tests/ee/ee_auto_generated_unit_tests/generated_tests.txt VOLTDB_GENERATED_TEST_PROGRAMS)
STRING(REPLACE "\n" ";" VOLTDB_GENERATED_TEST_PROGRAMS ${VOLTDB_GENERATED_TEST_PROGRAMS})

STRING(STRIP "${VOLTDB_GENERATED_TEST_PROGRAMS}" VOLTDB_GENERATED_TEST_PROGRAMS)
# MESSAGE( "VOLTDB_GENERATED_TEST_PROGRAMS is ${VOLTDB_GENERATED_TEST_PROGRAMS}" )

#
# The list of all test programs is the union of
# the manual tests and the generated tests.  We
# do some calculation to split them up into directory
# names and test names.
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
  # FALSE TRUE means we don't expect a failure, and to
  # run valgrind in a memcheck build.
  ADD_TEST_PROGRAM(${PROG} FALSE TRUE VOLTDB_TEST_PROGRAMS TEST)
ENDFOREACH()
FOREACH(PROG ${VOLTDB_GENERATED_TEST_PROGRAMS})
  # FALSE TRUE means we don't expect a failure and run valgrind
  # in a memcheck build.
  #
  # The difference between this and the last one is that these
  # are generated.
  ADD_TEST_PROGRAM(${PROG} FALSE TRUE VOLTDB_TEST_PROGRAMS TEST)
  # MESSAGE("TEST ${TEST} IS EXPECTED TO FAIL: ${VOLTDB_TESTFAIL_${TEST}}")
  # MESSAGE("TEST ${TEST}: VOLTDB_TESTDIR_${TEST} == ${VOLTDB_TESTDIR_${TEST}}")
  # MESSAGE("TEST ${TEST}: VOLTDB_TESTGEN_${TEST} == ${VOLTDB_TESTGEN_${TEST}}")
ENDFOREACH()

FOREACH(PROG ${VOLTDB_MANUAL_FAILING_TEST_PROGRAMS})
  # TRUE TRUE means we do expect a failure, and to run valgrind
  # in a memcheck build.
  ADD_TEST_PROGRAM(${PROG} TRUE TRUE VOLTDB_TEST_PROGRAMS TEST)
ENDFOREACH()
FOREACH(PROG ${VOLTDB_MANUAL_VALGRIND_FAILING_TEST_PROGRAMS})
  # If IS_VALGRIND_BUILD is TRUE then we
  # expect a failure.  Otherwise we don't.
  # The TRUE says to run valgrind.
  ADD_TEST_PROGRAM(${PROG} ${IS_VALGRIND_BUILD} TRUE VOLTDB_TEST_PROGRAMS TEST)
ENDFOREACH()
FOREACH(PROG ${VOLTDB_NOVALGRIND_TEST_PROGRAMS})
  # FALSE FALSE means we don't expect a failure, but don't run valgrind
  # in a memcheck build.
  ADD_TEST_PROGRAM(${PROG} FALSE FALSE VOLTDB_TEST_PROGRAMS TEST)
ENDFOREACH()

# The test harness is its own OBJECT library target.  It's
# just one file now, but it could be more.
VOLTDB_ADD_LIBRARY(voltdb_test_harness
  OBJECT
  ${CMAKE_SOURCE_DIR}/tests/ee/harness
  )

# MESSAGE("VoltDB_Tests: PROJECT_NAME Is ${PROJECT_NAME}")
# MESSAGE("VoltDB_Tests: PROJECT_SOURCE_DIRECTORY is ${${PROJECT_NAME}_SOURCE_DIR}")
TARGET_INCLUDE_DIRECTORIES(voltdb_test_harness
  PUBLIC
  ${CMAKE_SOURCE_DIR}/third_party/cpp
  ${${PROJECT_NAME}_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/src/ee
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/3pty-install/include
  )

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
  ADD_CUSTOM_TARGET(run-${TEST_DIR}-tests
    COMMAND /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -j ${VOLTDB_CORE_COUNT} -L ${TEST_DIR})
  ADD_CUSTOM_TARGET(build-${TEST_DIR}-tests)
  ADD_CUSTOM_COMMAND(TARGET build-${TEST_DIR}-tests
    PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory cpptests/${TEST_DIR}
    )
  ADD_DEPENDENCIES(build-all-tests build-${TEST_DIR}-tests)
ENDFOREACH()

########################################################################
#
# For each test, on the VOLTDB_TEST_ALLTESTS list, generate
# targets, commands and dependences.
#
########################################################################

# MESSAGE("VOLTDB_TEST_ALLTESTS IS ${VOLTDB_TEST_ALLTESTS}")
FOREACH (TEST_NAME ${VOLTDB_TEST_ALLTESTS})
  DEFINE_TEST(${TEST_NAME})
ENDFOREACH()
