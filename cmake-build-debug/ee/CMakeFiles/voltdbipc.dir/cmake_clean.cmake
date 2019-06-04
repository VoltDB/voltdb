file(REMOVE_RECURSE
  "voltdbipc.pdb"
  "voltdbipc"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/voltdbipc.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
