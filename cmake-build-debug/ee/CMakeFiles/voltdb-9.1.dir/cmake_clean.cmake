file(REMOVE_RECURSE
  "libvoltdb-9.1.pdb"
  "libvoltdb-9.1.jnilib"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/voltdb-9.1.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
