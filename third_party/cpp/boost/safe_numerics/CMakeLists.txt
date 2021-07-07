####################
# add include headers to IDE

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

file(GLOB include_files 
  RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" 
  "*.hpp"
)
add_custom_target(safe_numerics SOURCES ${include_files})

add_subdirectory("concept")

# end headers in IDE
####################
