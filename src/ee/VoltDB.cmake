FUNCTION(VALGRIND_FILE_NAME TARGET_DIR TARGET_NAME OUTPUT_VAR)
  SET(${OUTPUT_VAR} valgrind_ee_${TARGET_DIR}_${TARGET_NAME}.xml PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(JOIN VALUES GLUE OUTPUT)
  STRING (REPLACE ";" "${GLUE}" _TMP_STR "${VALUES}")
  SET (${OUTPUT} "${_TMP_STR}" PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(VALGRIND_COMMAND TARGET_DIR TARGET_NAME TEST_EXE_CMD WILL_FAIL OUTPUT_VAR OUTPUT_IS_VALGRIND_TEST)
  SET(SUPPRESSIONS ${VOLTDB_ROOT}/tests/ee/test_utils/vdbsuppressions.supp)
  IF ( WILL_FAIL )
    SET(FAIL_ARG --expect-fail=true)
  ELSE()
    SET(FAIL_ARG --expect-fail=false)
  ENDIF()
  IF ( IS_VALGRIND_BUILD )
    VALGRIND_FILE_NAME(${TARGET_DIR} ${TARGET_NAME} VGFN)
    SET(CMD_LIST
      ${CMAKE_SOURCE_DIR}/tools/runvalgrindtest.py
      ${FAIL_ARG}
      "/usr/bin/env"
      "valgrind"
      "--leak-check=full"
      "--show-reachable=yes"
      "--show-leak-kinds=all"
      "--errors-for-leak-kinds=all"
      "--track-origins=yes"
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
  FILE(GLOB PYTHON_SCRIPTS ${VOLTDB_ROOT}/tests/ee/${TEST_DIR}/${TEST_NAME}.py)
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
    SET(${OUTPUT_VARIABLE} ${VOLTDB_CORE_COUNT} PARENT_SCOPE)
  ENDIF()
ENDFUNCTION()

FUNCTION(FIND_VOLTDB_ROOT OUTPUT_VAR)
  EXECUTE_PROCESS(COMMAND bash -c "${CMAKE_SOURCE_DIR}/tools/findvdbroot.sh"
                  OUTPUT_VARIABLE ROOT)
  STRING(STRIP ${ROOT} ROOT)
  # MESSAGE("ROOT is ${ROOT}")
  # MESSAGE("OUTPUT_VAR is ${OUTPUT_VAR}")
  SET(${OUTPUT_VAR} ${ROOT} PARENT_SCOPE)
ENDFUNCTION()


