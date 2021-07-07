####################
# add include headers to IDE


find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

set(USE_FOLDERS TRUE)

file(GLOB include_files
  RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp"
)
add_custom_target(concept SOURCES ${include_files})
set_target_properties(concept PROPERTIES FOLDER "safe_numerics")

# end headers in IDE
####################
