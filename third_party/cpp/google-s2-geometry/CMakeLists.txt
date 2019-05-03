cmake_minimum_required(VERSION 2.8)
project(s2-geometry)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(CheckCXXCompilerFlag)
include(FindPackageHandleStandardArgs)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

set(OPENSSL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
set(OPENSSL_LIBRARIES   "${CMAKE_INSTALL_PREFIX}/lib/libcrypto.a")

message("OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}")

# These are set by the ant task.
# set(VOLTDB_SOURCE_DIR "../../..")
# set(VOLTDB_THIRD_PARTY_DIR "${VOLTDB_SOURCE_DIR}/third_party")
# set(VOLTDB_THIRD_PARTY_CPP_DIR "${VOLTDB_SOURCE_DIR}/third_party/cpp")
if (NOT DEFINED VOLTDB_BUILD_TESTS)
  set(VOLTDB_BUILD_TESTS FALSE)
endif()
if (NOT DEFINED VOLTDB_THIRD_PARTY_CPP_DIR)
  message(FATAL_ERROR "The variable VOLTDB_THIRD_PARTY_CPP_DIR needs to be set.")
endif (NOT DEFINED VOLTDB_THIRD_PARTY_CPP_DIR)

if ("${CXX_VERSION_FLAG}" STREQUAL "")
  message(FATAL_ERROR "No CXX_VERSION Flag set!!")
endif()

ADD_DEFINITIONS (-Wno-deprecated-declarations -Wno-conversion -DNDEBUG ${CXX_VERSION_FLAG})

message("OpenSSL DIR is ${OPENSSL_INCLUDE_DIR}")
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(geometry)
include_directories(geometry/include)
include_directories(geometry/util/endian)

file(GLOB S2Glob geometry/s2.cc geometry/s1interval.cc geometry/base/strtoint.cc geometry/util/math/exactfloat/exactfloat.cc geometry/strings/strutil.cc geometry/strings/split.cc geometry/base/stringprintf.cc geometry/s2cap.cc geometry/s1angle.cc geometry/s2cell.cc geometry/s2cellunion.cc geometry/s2cellid.cc geometry/s2edgeindex.cc geometry/s2edgeutil.cc geometry/s2latlngrect.cc geometry/s2loop.cc geometry/s2pointregion.cc geometry/s2latlng.cc geometry/s2polygon.cc geometry/s2polygonbuilder.cc geometry/s2polyline.cc geometry/s2r2rect.cc geometry/s2regionunion.cc geometry/s2region.cc geometry/s2regioncoverer.cc geometry/s2regionintersection.cc geometry/base/logging.cc geometry/util/coding/*.cc)

add_library(s2geo STATIC ${S2Glob})

file(GLOB InstallGlob geometry/include/s2geo/*.h)
file(GLOB BaseGlob geometry/include/s2geo/base/*.h)
file(GLOB StringsGlob geometry/include/s2geo/strings/*.h)
file(GLOB UtilCodingGlob geometry/include/s2geo/util/coding/*.h)
file(GLOB UtilEndianGlob geometry/include/s2geo/util/endian/*.h)
file(GLOB UtilHashGlob geometry/include/s2geo/util/hash/*.h)
file(GLOB UtilMathGlob geometry/include/s2geo/util/math/*.h)
file(GLOB UtilExactFloatGlob geometry/include/s2geo/util/math/exactfloat/*.h)

install(FILES ${InstallGlob} DESTINATION        include/s2geo)
install(FILES ${BaseGlob} DESTINATION           include/s2geo/base)
install(FILES ${StringsGlob} DESTINATION        include/s2geo/strings)
install(FILES ${UtilCodingGlob} DESTINATION     include/s2geo/util/coding)
install(FILES ${UtilEndianGlob} DESTINATION     include/s2geo/util/endian)
install(FILES ${UtilHashGlob} DESTINATION       include/s2geo/util/hash)
install(FILES ${UtilMathGlob} DESTINATION       include/s2geo/util/math)
install(FILES ${UtilExactFloatGlob} DESTINATION include/s2geo/util/math/exactfloat)
install(TARGETS s2geo DESTINATION lib)

if (CMAKE_USE_PTHREADS_INIT)  # The pthreads library is available and allowed.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGTEST_HAS_PTHREAD=1")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGTEST_HAS_PTHREAD=0")
endif()
if (APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-absolute-value -Wno-macro-redefined")
endif(APPLE)

if (${CMAKE_VERSION} VERSION_GREATER 2.8.12.999)
  cmake_policy(SET CMP0037 OLD) # allow underscores in targets
endif()

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpic")
  set(PTHREAD_LIBRARY_FOR_TESTS pthread)
endif()

