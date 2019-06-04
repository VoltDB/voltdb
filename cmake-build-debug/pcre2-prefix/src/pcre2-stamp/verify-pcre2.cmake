# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

if("/Users/russelhu/Github/voltdb/third_party/cpp/tarballs/pcre2-10.33.tar.gz" STREQUAL "")
  message(FATAL_ERROR "LOCAL can't be empty")
endif()

if(NOT EXISTS "/Users/russelhu/Github/voltdb/third_party/cpp/tarballs/pcre2-10.33.tar.gz")
  message(FATAL_ERROR "File not found: /Users/russelhu/Github/voltdb/third_party/cpp/tarballs/pcre2-10.33.tar.gz")
endif()

if("" STREQUAL "")
  message(WARNING "File will not be verified since no URL_HASH specified")
  return()
endif()

if("" STREQUAL "")
  message(FATAL_ERROR "EXPECT_VALUE can't be empty")
endif()

message(STATUS "verifying file...
     file='/Users/russelhu/Github/voltdb/third_party/cpp/tarballs/pcre2-10.33.tar.gz'")

file("" "/Users/russelhu/Github/voltdb/third_party/cpp/tarballs/pcre2-10.33.tar.gz" actual_value)

if(NOT "${actual_value}" STREQUAL "")
  message(FATAL_ERROR "error:  hash of
  /Users/russelhu/Github/voltdb/third_party/cpp/tarballs/pcre2-10.33.tar.gz
does not match expected value
  expected: ''
    actual: '${actual_value}'
")
endif()

message(STATUS "verifying file... done")
