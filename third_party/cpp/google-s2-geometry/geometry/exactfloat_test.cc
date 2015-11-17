/*
 * exactfloat_test.cc
 *
 *  Created on: Nov 10, 2015
 *      Author: bwhite
 */
#include "s2geo/testing/base/public/gunit.h"
#include "s2geo/util/math/exactfloat/exactfloat.h"
#include "s2geo/util/math/vector3.h"

/*
 * This is copied from s2.cc.
 */
typedef Vector3<ExactFloat> Vector3_xf;

class ExactFloatTestBase : public testing::Test {
protected:
  ExactFloat ef_zero;
  ExactFloatTestBase() :
    ef_zero(0.0)
  {}
};


TEST_F(ExactFloatTestBase, sanity) {
	EXPECT_EQ(ExactFloat(0.0), ef_zero);
}

