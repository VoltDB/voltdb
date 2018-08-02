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
       SET_TARGET_PROPERTIES(${NAME}
                             PROPERTIES
                             SUFFIX ".jnilib")
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
  -Wno-ignored-qualifiers -fno-strict-aliasing
  -DVOLT_LOG_LEVEL=${VOLT_LOG_LEVEL}
  -D_USE_MATH_DEFINES
)

IF ( ${VOLT_POOL_CHECKING} )
  VOLTDB_ADD_COMPILE_OPTIONS(-DVOLT_POOL_CHECKING=1)
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
# Ubuntu 17.10      7.2.0     3.9.1
#
# We should have a similar table for the mac, but apparently we
# don't.  We do have some empirical evidence that some configurations
# will build and run correctly.
#
########################################################################
SET (VOLTDB_COMPILER_U18p04 "7.3.0")
SET (VOLTDB_COMPILER_U17p10 "7.2.0")
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
#
# Note: Update this when adding a new compiler support.
#
SET (VOLTDB_COMPILER_NEWEST ${VOLTDB_COMPILER_U18p04})
#
#
#
MESSAGE("Using compiler ${CMAKE_CXX_COMPILER_ID}")
IF (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  SET (VOLTDB_LINK_FLAGS ${VOLTDB_LINK_FLAGS} -pthread)
  SET (VOLTDB_IPC_LINK_FLAGS ${VOLTDB_LIB_LINK_FLAGS} -rdynamic)
  VOLTDB_ADD_COMPILE_OPTIONS(-pthread -Wno-deprecated-declarations  -Wno-unknown-pragmas)
  # It turns out to be easier to go from a higher version to a lower
  # version, since we can't easily test <= and >=.
  IF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_NEWEST )
    # COMPILER_VERSION > 7.3.0
    MESSAGE ("GCC Version ${CMAKE_CXX_COMPILER_VERSION} is not verified for building VoltDB.")
    MESSAGE ("We're using the options for ${CMAKE_COMPILER_NEWEST}, which is the newest one we've tried.  Good Luck.")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-array-bounds)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U17p10 )
    # 7.2.0 < COMPILER_VERSION <= 7.3.0
    MESSAGE("Using the Ubuntu 17.10 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-array-bounds)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U17p04 )
    # < 6.3.0 COMPILER_VERSION <= 7.2.0
    MESSAGE("Using the Ubuntu 17.10 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-array-bounds)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U16p10)
    # 6.2.0 < COMPILER_VERSION and COMPILER_VERSION <= 6.3.0
    MESSAGE("Using the Ubuntu 17.04 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U16p04 )
    # 5.4.0 < COMPILER_VERSION and COMPILER_VERSION <= 6.2.0
    MESSAGE("Using the Ubuntu 16.10 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-local-typedefs -Wno-array-bounds)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U15p10 )
    # 5.2.1 < COMPILER_VERSION and COMPILER_VERSION <= 5.4.0
    MESSAGE("Using the Ubuntu 16.04 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS( -Wno-unused-local-typedefs )
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U15p04  )
    # 4.9.2 < COMPILER_VERSION and COMPILER_VERSION <= 5.2.1
    MESSAGE("Using the Ubuntu 15.10 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS( -Wno-unused-local-typedefs )
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_U14p04 )
    # 4.8.4 < COMPILER_VERSION and COMPILER_VERSION <= 4.9.2
    # Note that U14.04 and C7 are different versions, but equivalent
    # for our needs here.
    # Nothing special added to the compile flags.
    MESSAGE("Using the Ubuntu 15.04 compiler settings for gcc ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-but-set-variable -Wno-unused-local-typedefs -Wno-float-conversion -Wno-conversion)
    SET (CXX_VERSION_FLAG -std=c++11)
  ELSEIF ( CMAKE_CXX_COMPILER_VERSION VERSION_GREATER VOLTDB_COMPILER_CXX0X)
    # 4.6.0 < COMPILER_VERSION and COMPILER_VERSION <= 4.8.4
    # Use -std=c++0x.  This is in GCC's experimental C++11 compiler
    # support version, which is sufficient for our use.
    MESSAGE("Using the Centos 6 settings for ${CMAKE_CXX_COMPILER_VERSION}")
    VOLTDB_ADD_COMPILE_OPTIONS(-Wno-unused-but-set-variable -Wno-unused-local-typedefs -Wno-float-conversion -Wno-conversion)
    SET (CXX_VERSION_FLAG -std=c++0x)
  ELSE()
    message(FATAL_ERROR "GNU Compiler version ${CMAKE_CXX_COMPILER_VERSION} is too old to build VoltdB.  Try at least ${VOLTDB_COMPILER_CXX0X}.")
  ENDIF()
ELSEIF (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # All versions of clang use C++11.
  SET (CXX_VERSION_FLAG -std=c++11)
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
