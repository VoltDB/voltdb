FUNCTION(VALGRIND_FILE_NAME TARGET_DIR TARGET_NAME OUTPUT_VAR)
  SET(${OUTPUT_VAR} valgrind_ee_${TARGET_DIR}_${TARGET_NAME}.xml PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(JOIN VALUES GLUE OUTPUT)
  STRING (REPLACE ";" "${GLUE}" _TMP_STR "${VALUES}")
  SET (${OUTPUT} "${_TMP_STR}" PARENT_SCOPE)
ENDFUNCTION()

FUNCTION(VALGRIND_COMMAND TARGET_DIR TARGET_NAME TEST_EXE_CMD WILL_FAIL OUTPUT_VAR)
  SET(SUPPRESSIONS ${CMAKE_SOURCE_DIR}/${VOLTDB_EE_TEST_DIR}/test_utils/vdbsuppressions.supp)
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
      "--error-exitcode=1"
      "--suppressions=${SUPPRESSIONS}"
      "--xml=yes"
      "--xml-file=${VGFN}"
      ${TEST_EXE_CMD})
    SET(${OUTPUT_VAR}
      ${CMD_LIST}
      PARENT_SCOPE)
  ELSE()
    SET(${OUTPUT_VAR} ${TEST_EXE_CMD} PARENT_SCOPE)
  ENDIF()
ENDFUNCTION()

#
# Calculate if this is a python test.  If there is a python script
# in the source folder called TEST_DIR/TEST_NAME.py, then we
# wrap the test command with that script by putting the command
# in the script's argument list.
#
FUNCTION(PYTHON_COMMAND TEST_DIR TEST_NAME TEST_EXE_CMD OUTPUT_VAR)
  FILE(GLOB PYTHON_SCRIPTS ${VOLTDB_ROOT}/tests/ee/${TEST_DIR}/${TEST_NAME}.py)
  MESSAGE("Looking for ${VOLTDB_ROOT}/tests/ee/${TEST_DIR}/${TEST_NAME}.py")
  LIST(LENGTH PYTHON_SCRIPTS IS_PYTHON)
  IF (${IS_PYTHON} GREATER 0)
    LIST(GET PYTHON_SCRIPTS 0 PYTHON_SCRIPT)
    LIST(INSERT TEST_EXE_CMD 0 ${PYTHON_SCRIPT})
    MESSAGE("Python Command for ${TEST_NAME} is ${TEST_EXE_CMD}")
  ENDIF()
  SET(${OUTPUT_VAR} ${TEST_EXE_CMD} PARENT_SCOPE)
ENDFUNCTION()

