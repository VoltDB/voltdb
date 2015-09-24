/// Copyright 2004 Google Inc.
/// All Rights Reserved.
///
///

#include <iostream>
using std::ostream;
using std::cout;
using std::endl;

#include "s2geo/base/int128.h"
#include "s2geo/base/integral_types.h"

const uint128 kuint128max(static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF)),
                          static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF)));

ostream& operator<<(ostream& o, const uint128& b) {
  return (o << b.hi_ << "::" << b.lo_);
}
