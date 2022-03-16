########################################################################
#
# Compiler Options
#
########################################################################
#
# We give compiler options in two parts.  One is the set of base
# options, which all compilations use.  The second is the set of
# compiler, platform, and version options.
#
# These are the base compiler options, and some
# other linker options.
#
########################################################################
########################################################################
#
# Common Compiler Flags
#
########################################################################
SET (VOLTDB_COMPILE_OPTIONS)
SET (CMAKE_EXPORT_COMPILE_COMMANDS ON)
FUNCTION (VOLTDB_ADD_COMPILE_OPTIONS)
  FOREACH(OPT ${ARGN})
    SET(VOLTDB_COMPILE_OPTIONS "${VOLTDB_COMPILE_OPTIONS} ${OPT}")
  ENDFOREACH()
  SET(VOLTDB_COMPILE_OPTIONS ${VOLTDB_COMPILE_OPTIONS} PARENT_SCOPE)
ENDFUNCTION()

FUNCTION (VOLTDB_ADD_LIBRARY NAME KIND)
  # MESSAGE("Defining ${NAME} kind ${KIND} SOURCES ${ARGN}")
  ADD_LIBRARY(${NAME} ${KIND} ${ARGN})
  SET_TARGET_PROPERTIES(${NAME}
    PROPERTIES
    COMPILE_FLAGS "${VOLTDB_COMPILE_OPTIONS}"
  )
  # Make shared libraries be .jnilib on the mac.
  IF ((${KIND} STREQUAL "SHARED") AND (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" ))
    SET_TARGET_PROPERTIES(${NAME} PROPERTIES SUFFIX ".jnilib")
  ENDIF()
ENDFUNCTION()

FUNCTION (VOLTDB_ADD_EXECUTABLE NAME)
  ADD_EXECUTABLE(${NAME} ${ARGN})
  SET_TARGET_PROPERTIES(${NAME}
    PROPERTIES
    COMPILE_FLAGS "${VOLTDB_COMPILE_OPTIONS}"
    )
ENDFUNCTION()

IF (IS_VALGRIND_BUILD)
  VOLTDB_ADD_COMPILE_OPTIONS(-g3 -DMEMCHECK)
  SET (VOLTDB_USE_VALGRIND --valgrind)
ELSEIF (VOLTDB_BUILD_TYPE STREQUAL "DEBUG")
  VOLTDB_ADD_COMPILE_OPTIONS(-g3)
ELSEIF (VOLTDB_BUILD_TYPE STREQUAL "RELEASE")
  VOLTDB_ADD_COMPILE_OPTIONS(-O3 -g3 -mmmx -msse -msse2 -msse3 -DNDEBUG)
ELSE()
  MESSAGE(FATAL_ERROR "BUILD TYPE ${VOLTDB_BUILD_TYPE} IS UNKNOWN.")
ENDIF()

VOLTDB_ADD_COMPILE_OPTIONS(
  -Wall -Wextra -Werror -Woverloaded-virtual
  -Wpointer-arith -Wcast-qual -Wwrite-strings
  -Winit-self -Wno-sign-compare -Wno-unused-parameter
  -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -DNOCLOCK
  -fno-omit-frame-pointer
  -fvisibility=default
  -DBOOST_SP_DISABLE_THREADS -DBOOST_DISABLE_THREADS -DBOOST_ALL_NO_LIB
  -Wno-deprecated-declarations  -Wno-unknown-pragmas
  -Wno-ignored-qualifiers
  -fno-strict-aliasing
  -DVOLT_LOG_LEVEL=${VOLT_LOG_LEVEL}
  -D_USE_MATH_DEFINES
)

IF ( ${VOLT_POOL_CHECKING} )
  VOLTDB_ADD_COMPILE_OPTIONS(-DVOLT_POOL_CHECKING=1)
ENDIF()

IF ( ${VOLT_TIMER_ENABLED} )
  VOLTDB_ADD_COMPILE_OPTIONS(-DVOLT_TIMER_ENABLED=1)
ENDIF()

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
# OS Ver.        gcc vers     cmake ver.   Clang version
# Ubuntu 14.04      4.8.4     2.8.12.2
# Centos7:          4.8.5     2.8.12.2
# Ubuntu 16.04      5.4.0     3.5.1
# Ubuntu 18.04      7.3.0     3.10.2
# Centos8:          7.6.4     3.11.4
#
# We should have a similar table for the mac, but apparently we
# don't.  We do have some empirical evidence that some configurations
# will build and run correctly.
#
########################################################################
SET (VOLTDB_COMPILER_U14p04 "4.8.4")
#
#
#
MESSAGE("Using compiler ${CMAKE_CXX_COMPILER_ID} version ${CMAKE_CXX_COMPILER_VERSION}")
IF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  SET (VOLTDB_LINK_FLAGS ${VOLTDB_LINK_FLAGS} -pthread)
  SET (VOLTDB_IPC_LINK_FLAGS ${VOLTDB_LIB_LINK_FLAGS} -rdynamic)
  VOLTDB_ADD_COMPILE_OPTIONS(-pthread -Wno-deprecated-declarations  -Wno-unknown-pragmas -Wno-unused-local-typedefs)

  # Some supported versions of cmake do not support VERSION_GREATER_EQUAL so use NOT ... VERSION_LESS
  IF (CMAKE_CXX_COMPILER_VERSION VERSION_LESS VOLTDB_COMPILER_U14p04)
    message(FATAL_ERROR "GNU Compiler version ${CMAKE_CXX_COMPILER_VERSION} is too old to build VoltdB.  Try at least ${VOLTDB_COMPILER_U14p04}.")
  ELSEIF (CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-but-set-variable -Wno-float-conversion -Wno-conversion)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSE()
    SET (CXX_VERSION_FLAG -std=c++14)

    IF (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "6")
      VOLTDB_ADD_COMPILE_OPTIONS(-Wno-array-bounds)

      IF (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "7")
        SET (CXX_VERSION_FLAG -std=c++17)

        IF (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "8")
          MESSAGE ("GCC Version ${CMAKE_CXX_COMPILER_VERSION} is not verified for building VoltDB.")
          VOLTDB_ADD_COMPILE_OPTIONS(-Wno-error=class-memaccess)

          # Only need to ignore deprecated-copy in a debug build
          IF (NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9" AND (VOLTDB_BUILD_TYPE STREQUAL "DEBUG" OR VOLTDB_BUILD_TYPE STREQUAL "MEMCHECK"))
             VOLTDB_ADD_COMPILE_OPTIONS(-Wno-deprecated-copy)
          ENDIF()
        ENDIF()
      ENDIF()
    ENDIF()
  ENDIF()
ELSEIF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  # All versions of clang use C++14.
  SET (CXX_VERSION_FLAG -std=c++14)
  MESSAGE("CXX_VERSION_FLAG is ${CXX_VERSION_FLAG}")
  IF ( ( "3.4.0" VERSION_LESS CMAKE_CXX_COMPILER_VERSION )
       AND ( CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.0.0" ) )
    # Some clang 3.4.x version
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-varargs)
  ELSEIF ( "7.0.0" VERSION_LESS ${CMAKE_CXX_COMPILER_VERSION} )
    # This is some odd mac version number.  It's not
    # related to the LLVM versioning numbers.
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-absolute-value)
  ENDIF()
  IF ( "9.0.0" VERSION_LESS ${CMAKE_CXX_COMPILER_VERSION} )
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-user-defined-warnings)
  ENDIF()
  IF ("13.1.5" VERSION_LESS ${CMAKE_CXX_COMPILER_VERSION} )
    # Later updates require this flag
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-deprecated-copy -Wno-unused-but-set-variable)
  ENDIF()
ELSE()
  MESSAGE (FATAL_ERROR "Unknown compiler family ${CMAKE_CXX_COMPILER_ID}.  We only support GNU and Clang.")
ENDIF ()

VOLTDB_ADD_COMPILE_OPTIONS(${CXX_VERSION_FLAG})

#
# Add VOLTDB_CXXFLAGS, supplied by the user.
#
VOLTDB_ADD_COMPILE_OPTIONS(${VOLTDB_CXXFLAGS})

IF ( ${CMAKE_SYSTEM_NAME} STREQUAL "Linux" )
  VOLTDB_ADD_COMPILE_OPTIONS(-Wno-attributes -Wcast-align -DLINUX -fpic)
  SET (VOLTDB_NM_OPTIONS "-n --demangle")
  IF (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    SET (VOLTDB_OPENSSL_TOKEN "linux-x86_64:gcc -fpic")
  ELSE()
    MESSAGE(FATAL_ERROR "Compiler family ${CMAKE_CXX_COMPILER_ID} is not supported when building VoltDB.")
  ENDIF()
ELSEIF( ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin" )
  SET (VOLTDB_NM_OPTIONS "-n")
  SET (VOLTDB_OPENSSL_TOKEN "darwin64-x86_64-cc")
  VOLTDB_ADD_COMPILE_OPTIONS("-DMACOSX")
ELSE()
  MESSAGE(FATAL_ERROR "System nameed ${CMAKE_SYSTEM_NAME} is unknown")
ENDIF()
