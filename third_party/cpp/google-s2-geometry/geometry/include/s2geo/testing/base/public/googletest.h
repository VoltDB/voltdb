/// Copyright 2010-2014, Google Inc.
/// All rights reserved.
///
/// Redistribution and use in source and binary forms, with or without
/// modification, are permitted provided that the following conditions are
/// met:
///
///     * Redistributions of source code must retain the above copyright
/// notice, this list of conditions and the following disclaimer.
///     * Redistributions in binary form must reproduce the above
/// copyright notice, this list of conditions and the following disclaimer
/// in the documentation and/or other materials provided with the
/// distribution.
///     * Neither the name of Google Inc. nor the names of its
/// contributors may be used to endorse or promote products derived from
/// this software without specific prior written permission.
///
/// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
/// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
/// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
/// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
/// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
/// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
/// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
/// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
/// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
/// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
/// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef MOZC_TESTING_BASE_PUBLIC_GOOGLETEST_H_
#define MOZC_TESTING_BASE_PUBLIC_GOOGLETEST_H_

#include "s2geo/base/flags.h"

/// --test_srcdir is the path to a directory that contains the input data files
/// for a test, so that each entry in the 'data' section of the BUILD rule for
/// this test specifies a path relative to FLAGS_test_srcdir.
DECLARE_string(test_srcdir);

/// --test_tmpdir is a temporary directory that you can write to from inside a
/// test.  Files you write will eventually be cleaned up but you can see them at
/// ~/local/tmp in the immediate aftermath of the test.  These files are
/// stored on local disk, not on the networked filer.
DECLARE_string(test_tmpdir);

namespace mozc {
  /// Initialize FLAGS_test_srcdir and FLAGS_test_tmpdir.
  void InitTestFlags();
}


#endif  // MOZC_TESTING_BASE_PUBLIC_GOOGLETEST_H_
