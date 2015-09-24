cmake_minimum_required(VERSION 2.8)
project(s2-geometry)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(CheckCXXCompilerFlag)
include(FindPackageHandleStandardArgs)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

set(ENV{OPENSSL_ROOT_DIR} "/usr/local/opt/openssl/include")
find_package(OpenSSL REQUIRED)

message("OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}")

# These are set by the ant task.
# set(VOLTDB_SOURCE_DIR "../../..")
# set(VOLTDB_THIRD_PARTY_DIR "${VOLTDB_SOURCE_DIR}/third_party")
# set(VOLTDB_THIRD_PARTY_CPP_DIR "${VOLTDB_SOURCE_DIR}/third_party/cpp")
if (NOT DEFINED VOLTDB_THIRD_PARTY_CPP_DIR)
  message(FATAL_ERROR "The variable VOLTDB_THIRD_PARTY_CPP_DIR needs to be set.")
endif (NOT DEFINED VOLTDB_THIRD_PARTY_CPP_DIR)
set(VOLTDB_GOOGLE_TEST_SRC_DIR "${VOLTDB_THIRD_PARTY_CPP_DIR}/googletest/googletest")

ADD_DEFINITIONS (-Wno-deprecated-declarations -DNDEBUG -std=c++11)

include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(geometry)
include_directories(geometry/include)
include_directories(geometry/util/endian)

file(GLOB S2Glob geometry/s2.cc geometry/s1interval.cc geometry/base/strtoint.cc geometry/util/math/exactfloat/exactfloat.cc geometry/strings/strutil.cc geometry/strings/split.cc geometry/base/stringprintf.cc geometry/s2cap.cc geometry/s1angle.cc geometry/s2cell.cc geometry/s2cellunion.cc geometry/s2cellid.cc geometry/s2edgeindex.cc geometry/s2edgeutil.cc geometry/s2latlngrect.cc geometry/s2loop.cc geometry/s2pointregion.cc geometry/s2latlng.cc geometry/s2polygon.cc geometry/s2polygonbuilder.cc geometry/s2polyline.cc geometry/s2r2rect.cc geometry/s2regionunion.cc geometry/s2region.cc geometry/s2regioncoverer.cc geometry/s2regionintersection.cc geometry/base/logging.cc geometry/util/coding/*.cc)

add_library(s2geo STATIC ${S2Glob})
# Add RPATH info to executables so that when they are run after being installed
# (i.e., from /usr/local/bin/) the linker can find library dependencies. For
# more info see http://www.cmake.org/Wiki/CMake_RPATH_handling
set_property(TARGET s2geo PROPERTY INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(s2testing STATIC geometry/s2testing.cc)

file(GLOB InstallGlob geometry/*.h)
file(GLOB BaseGlob geometry/base/*.h)
file(GLOB UtilMathGlob geometry/util/math/*.h)
file(GLOB TestGlob geometry/*_test.cc)

install(FILES ${InstallGlob} DESTINATION include/s2geo)
install(FILES ${BaseGlob} DESTINATION include/s2geo/base)
install(FILES ${UtilMathGlob} DESTINATION include/util/math)
install(TARGETS s2geo DESTINATION lib)

if (CMAKE_USE_PTHREADS_INIT)  # The pthreads library is available and allowed.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGTEST_HAS_PTHREAD=1")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGTEST_HAS_PTHREAD=0")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-absolute-value -Wno-macro-redefined")
set(GTEST_DIR "${VOLTDB_GOOGLE_TEST_SRC_DIR}")
set(GTEST_INCLUDE_DIRECTORIES ${GTEST_DIR}/include ${GTEST_DIR} ${GTEST_DIR}/src)
include_directories(${GTEST_INCLUDE_DIRECTORIES})
add_library(gtest ${GTEST_DIR}/src/gtest-all.cc ${GTEST_DIR}/src/gtest_main.cc)
enable_testing(true)

cmake_policy(SET CMP0037 OLD) # allow underscores in targets
foreach( testsourcefile ${TestGlob} )
    get_filename_component(testname ${testsourcefile} NAME_WE)
    add_executable( ${testname} ${testsourcefile} )
    target_link_libraries( ${testname} s2geo s2testing gtest ${OPENSSL_LIBRARIES})
    add_test( ${testname} ${testname} )
endforeach()
