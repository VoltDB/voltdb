########################################################################
#
# These are some functions we use when using CMake.
#
########################################################################
FUNCTION(BANNER)
  MESSAGE("######################################################################")
  MESSAGE("#")
  FOREACH(MSG ${ARGN})
    MESSAGE("# ${MSG}")
  ENDFOREACH()
  MESSAGE("#")
  MESSAGE("######################################################################")
ENDFUNCTION()

FUNCTION(VALGRIND_FILE_NAME TARGET_DIR TARGET_NAME OUTPUT_VAR)
  SET(${OUTPUT_VAR} valgrind_ee_${TARGET_DIR}_${TARGET_NAME}.xml PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(VALGRIND_COMMAND TARGET_DIR TARGET_NAME TEST_EXE_CMD WILL_FAIL OUTPUT_VAR OUTPUT_IS_VALGRIND_TEST)
  SET(SUPPRESSIONS ${${PROJECT_NAME}_SOURCE_DIR}/test_utils/vdbsuppressions.supp)
  IF ( WILL_FAIL )
    SET(FAIL_ARG --expect-fail=true)
  ELSE()
    SET(FAIL_ARG --expect-fail=false)
  ENDIF()
  IF ( IS_VALGRIND_BUILD )
    VALGRIND_FILE_NAME(${TARGET_DIR} ${TARGET_NAME} VGFN)
    # It would be good if we could add some different parameters,
    # such as --show-leak-kinds=all, --errors-for-leak-kinds=all and
    # --track-origins.  But these apparently are not available in
    # the really old version of valgrind on Centos 6.
    #
    SET(CMD_LIST
      ${CMAKE_SOURCE_DIR}/tools/runvalgrindtest.py
      ${FAIL_ARG}
      "/usr/bin/env"
      "valgrind"
      "--leak-check=full"
      "--show-reachable=yes"
      "--error-exitcode=1"
      "--suppressions=${SUPPRESSIONS}"
      "--xml=yes"
      "--xml-file=${VGFN}"
      ${TEST_EXE_CMD})
    SET(${OUTPUT_VAR} ${CMD_LIST} PARENT_SCOPE)
    SET(${OUTPUT_IS_VALGRIND_TEST} 1)
  ELSE()
    SET(${OUTPUT_VAR} ${TEST_EXE_CMD} PARENT_SCOPE)
    SET(${OUTPUT_IS_VALGRIND_TEST} 0 PARENT_SCOPE)
  ENDIF()
ENDFUNCTION()

#
# Calculate if this is a python test.  If there is a python script
# in the source folder called TEST_DIR/TEST_NAME.py, then we
# wrap the test command with that script by putting the command
# in the script's argument list.
#
FUNCTION(PYTHON_COMMAND TEST_DIR TEST_NAME TEST_EXE_CMD OUTPUT_VAR OUTPUT_IS_PYTHON_TEST)
  FILE(GLOB PYTHON_SCRIPTS ${${PROJECT_NAME}_SOURCE_DIR}/${TEST_DIR}/${TEST_NAME}.py)
  LIST(LENGTH PYTHON_SCRIPTS IS_PYTHON)
  IF (${IS_PYTHON} GREATER 0)
    LIST(GET PYTHON_SCRIPTS 0 PYTHON_SCRIPT)
    LIST(INSERT TEST_EXE_CMD 0 "${PYTHON_SCRIPT}")
    SET(${OUTPUT_IS_PYTHON_TEST} 1 PARENT_SCOPE)
    # MESSAGE("Python Command for ${TEST_NAME} is ${TEST_EXE_CMD}")
  ELSE()
    SET(${OUTPUT_IS_PYTHON_TEST} 0 PARENT_SCOPE)
  ENDIF()
  SET(${OUTPUT_VAR} ${TEST_EXE_CMD} PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(COMPUTE_CORE_COUNT OUTPUT_VARIABLE)
  IF ( ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" )
    # Surely there is some way to discover this.
    SET (VOLTDB_CORE_COUNT 4)
  ELSE()
    EXECUTE_PROCESS(COMMAND bash -c "grep '^processor' /proc/cpuinfo | wc -l"
                    OUTPUT_VARIABLE VOLTDB_CORE_COUNT)
    STRING(STRIP "${VOLTDB_CORE_COUNT}" VOLTDB_CORE_COUNT)
    IF ( "${VOLTDB_CORE_COUNT}" STREQUAL "" )
        MESSAGE(FATAL_ERROR "Cannot calculate the core count.")
    ENDIF()
  ENDIF()
  SET(${OUTPUT_VARIABLE} ${VOLTDB_CORE_COUNT} PARENT_SCOPE)
ENDFUNCTION()

#
# Parse the TEST_NAME and decide what directory the
# source and executable should be in, what the command
# should be and how to run it if it's a valgrind build.
#
FUNCTION (ADD_TEST_PROGRAM TEST_NAME EXPECT_FAILURE RUN_VALGRIND OUTPUT_VAR OUTPUT_TEST_NAME)
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
  SET(VOLTDB_TEST_ALLTESTS ${VOLTDB_TEST_ALLTESTS} PARENT_SCOPE)
  # Find if this test directory has been
  # seen before.  If not, then append the
  # name to the test_dir_list.
  LIST(FIND VOLTDB_TEST_DIR_LIST ${DIR} FIND_IDX)
  IF (${FIND_IDX} LESS 0)
    LIST(APPEND VOLTDB_TEST_DIR_LIST ${DIR})
  ENDIF()
  SET("VOLTDB_TESTDIR_${TEST}" ${DIR} PARENT_SCOPE)
  SET("VOLTDB_TESTGEN_${TEST}" ${GENERATED} PARENT_SCOPE)
  SET("VOLTDB_TESTFAIL_${TEST}" ${EXPECT_FAILURE} PARENT_SCOPE)
  SET("VOLTDB_RUNVALGRIND_${TEST}" ${RUN_VALGRIND} PARENT_SCOPE)
  SET(VOLTDB_TEST_DIR_LIST ${VOLTDB_TEST_DIR_LIST} PARENT_SCOPE)
  SET(${OUTPUT_TEST_NAME} ${TEST} PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(DEFINE_TEST TEST_NAME)
  SET(TEST_DIR ${VOLTDB_TESTDIR_${TEST_NAME}})
  SET(TEST_GEN ${VOLTDB_TESTGEN_${TEST_NAME}})
  # MESSAGE("Defining test ${TEST_DIR}/${TEST_NAME} (from ${TEST_GEN})")
  #
  # Define the test executable, and set the build-all-tests
  # depends on the test.  Remember the necessary include
  # directories and link libraries.
  #
  # First, calculate the actual test command.  We
  # don't need this to define the executable, but we do
  # need it to define the output name of the executable.
  #
  VOLTDB_ADD_EXECUTABLE(${TEST_NAME}
      $<TARGET_OBJECTS:voltdb_test_harness>
      $<TARGET_OBJECTS:voltdbobjs>
      $<TARGET_OBJECTS:third_party_objs>
      ${${PROJECT_NAME}_SOURCE_DIR}/${TEST_GEN}/${TEST_DIR}/${TEST_NAME}.cpp)
  ADD_CUSTOM_COMMAND(TARGET ${TEST_NAME}
    PRE_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/cpptests/${TEST_DIR}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/tests/test_working_dir
    )
  SET(TARGET_EXE_DIR ${CMAKE_BINARY_DIR}/cpptests/${TEST_DIR})
  SET(TARGET_EXE_CMD ${TARGET_EXE_DIR}/${TEST_NAME})
  SET_TARGET_PROPERTIES(${TEST_NAME}
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TARGET_EXE_DIR}
    EXCLUDE_FROM_ALL TRUE)
  TARGET_INCLUDE_DIRECTORIES(${TEST_NAME}
    PUBLIC
    ${CMAKE_SOURCE_DIR}/third_party/cpp
    ${${PROJECT_NAME}_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src/ee
    ${CMAKE_BINARY_DIR}/3pty-install/include
    )
  TARGET_LINK_LIBRARIES(${TEST_NAME}
    ${VOLTDB_LINK_FLAGS}
    -L${CMAKE_BINARY_DIR}/3pty-install/lib
    -lpcre2-8 -ls2geo -lcrypto
    -ldl
    )
  ADD_DEPENDENCIES(${TEST_NAME}
    pcre2 crypto s2geo ${VOLTDB_LIBNAME})

  # This allows us to run "make build-sometest" to build sometest.
  ADD_CUSTOM_TARGET(build-${TEST_NAME}
    DEPENDS ${TEST_NAME})
  SET(WILL_FAIL ${VOLTDB_TESTFAIL_${TEST_NAME}})
  # If we are running a valgrind test, then calculate the
  # valgrind command.  The output variable, VALGRIND_EXE_CMD,
  # may be the same as TARGET_EXE or else it may be a call
  # to valgrind.
  IF (${VOLTDB_RUNVALGRIND_${TEST_NAME}})
    VALGRIND_COMMAND(${TEST_DIR} ${TEST_NAME} ${TARGET_EXE_CMD} ${WILL_FAIL} VALGRIND_EXE_CMD IS_VALGRIND_TEST)
  ELSE()
    SET(VALGRIND_EXE_CMD ${TARGET_EXE_CMD})
  ENDIF()
  # Some tests need to run python.  This needs to be
  # second because we want to run valgrind on the actual
  # test executable, TARGET_EXE_CMD, and not on python.
  PYTHON_COMMAND(${TEST_DIR} ${TEST_NAME} "${VALGRIND_EXE_CMD}" CTEST_EXE_CMD OUTPUT_IS_PYTHON_TEST)
  # So, "make build-${TEST_NAME}"" builds the test and
  # "make run-${TEST_NAME}" runs the single test.
  ADD_DEPENDENCIES(build-${TEST_DIR}-tests build-${TEST_NAME})
  ADD_DEPENDENCIES(run-${TEST_DIR}-tests build-${TEST_NAME})
  # MESSAGE("Defining target run-${TEST_NAME}")
  ADD_CUSTOM_TARGET(run-${TEST_NAME}
    DEPENDS ${TEST_NAME}
    COMMAND /usr/bin/env CTEST_OUTPUT_ON_FAILURE=true ${CMAKE_CTEST_COMMAND} -j ${VOLTDB_CORE_COUNT} -R ${TEST_NAME})
  # Some tests are expected to fail.  Also, tag the
  # test with its label.
  # MESSAGE("Test ${TEST_NAME} has command ${CTEST_EXE_CMD}.")
  ADD_TEST(NAME ${TEST_NAME}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests/test_working_dir
    COMMAND ${CTEST_EXE_CMD})
  IF (${TEST_GEN} STREQUAL ".")
    SET(MGLABEL "manual")
    ADD_DEPENDENCIES(run-manual-tests run-${TEST_NAME})
    ADD_DEPENDENCIES(build-manual-tests build-${TEST_NAME})
  ELSE()
    SET(MGLABEL "generated")
    ADD_DEPENDENCIES(run-generated-tests run-${TEST_NAME})
    ADD_DEPENDENCIES(build-generated-tests build-${TEST_NAME})
  ENDIF()
  SET_TESTS_PROPERTIES(${TEST_NAME}
    PROPERTIES
    WILL_FAIL ${WILL_FAIL}
    LABELS "${TEST_DIR};${MGLABEL}")
ENDFUNCTION()
