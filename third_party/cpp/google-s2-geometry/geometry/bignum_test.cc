/*
 * bignum_test.cc
 *
 *  Created on: Nov 6, 2015
 *      Author: bill.white@voltdb.com
 */
#include "s2geo/util/math/exactfloat/bignum.h"
#include "s2geo/testing/base/public/gunit.h"

struct BignumTestBase : public testing::Test {
    BIGNUM bn_const_zero{0};
    BIGNUM bn_const_default;

    const BIGNUM bn_const_1                     {  1};
    const BIGNUM bn_const_2                     {  2};
    const BIGNUM bn_const_3                     {  3};
    const BIGNUM bn_const_4                     {  4};
    const BIGNUM bn_const_5                     {  5};
    const BIGNUM bn_const_6                     {  6};
    const BIGNUM bn_const_7                     {  7};
    const BIGNUM bn_const_8                     {  8};
    const BIGNUM bn_const_9                     {  9};
    const BIGNUM bn_const_16                    { 16};

    const BIGNUM bn_const_m1                    { -1};
    const BIGNUM bn_const_m2                    { -2};
    const BIGNUM bn_const_m3                    { -3};
    const BIGNUM bn_const_m4                    { -4};
    const BIGNUM bn_const_m5                    { -5};
    const BIGNUM bn_const_m6                    { -6};
    const BIGNUM bn_const_m7                    { -7};
    const BIGNUM bn_const_m8                    { -8};
    const BIGNUM bn_const_m9                    { -9};
    const BIGNUM bn_const_m16                   {-16};

    const BIGNUM bn_const_100                   { 100};
    const BIGNUM bn_const_100_too               { 100};
    const BIGNUM bn_const_200                   { 200};
    const BIGNUM bn_const_300                   { 300};
    const BIGNUM bn_const_20000                 { 20000};
    const BIGNUM bn_const_30000                 { 30000};
    const BIGNUM bn_const_60000                 { 60000};

    const BIGNUM bn_const_m100                  {-100};
    const BIGNUM bn_const_m100_too              {-100};
    const BIGNUM bn_const_m200                  {-200};
    const BIGNUM bn_const_m300                  {-300};
    const BIGNUM bn_const_m20000                {-20000};
    const BIGNUM bn_const_m30000                {-30000};
    const BIGNUM bn_const_m60000                {-60000};

    BIGNUM bn_00;
    BIGNUM bn_01;
    BIGNUM bn_02;
    BIGNUM bn_03;
    BIGNUM bn_04;
    BIGNUM bn_05;
    BIGNUM bn_06;
    BIGNUM bn_07;
    BIGNUM bn_08;
    BIGNUM bn_09;
    BIGNUM bn_10;

    BignumTestBase() {
    }
};

/*************************************************************************
 * First, we have some tests of the member functions.
 *************************************************************************/
/*
 * Can we create two BIGNUM values and compare them
 * for equality.
 */
TEST_F(BignumTestBase, TestSanity) {
    EXPECT_EQ(bn_const_100, bn_const_100_too);
    EXPECT_NE(bn_const_100, bn_const_200);
    EXPECT_NE(bn_const_200, bn_const_m200);
    EXPECT_EQ(bn_const_zero, bn_const_default);
}

TEST_F(BignumTestBase, TestToString) {
    EXPECT_TRUE(bn_const_m200.is_negative());
    const char *p1 = bn_const_100.toString();
    const char *p2 = bn_const_m200.toString();
    const char *p3 = bn_const_zero.toString();
    EXPECT_STREQ(p1, (const char *)"100");
    EXPECT_STREQ(p2, (const char *)"-200");
    EXPECT_STREQ(p3, (const char *)"0");
    /*
     * Make sure we don't get any crashes here.
     */
    BIGNUM::disposeString(p1);
    EXPECT_TRUE(true);
    BIGNUM::disposeString(p2);
    EXPECT_TRUE(true);
    BIGNUM::disposeString(p3);
    EXPECT_TRUE(true);
}

TEST_F(BignumTestBase, TestAdd) {
    bn_00.add(bn_const_100, bn_const_200);
    EXPECT_EQ(bn_00, bn_const_300);
    bn_00.add(bn_const_100, bn_const_m100);
    EXPECT_EQ(bn_00, bn_const_zero);
    EXPECT_TRUE(bn_00.is_zero());
    bn_00.add(100);
    EXPECT_EQ(bn_00, bn_const_100);
}

TEST_F(BignumTestBase, TestMul) {
    bn_00.mul(bn_const_100, bn_const_200);
    EXPECT_EQ(bn_00, bn_const_20000);
    bn_00.mul(bn_const_m100, bn_const_200);
    EXPECT_EQ(bn_00, bn_const_m20000);
    bn_00.mul(bn_const_zero, bn_const_200);
    EXPECT_EQ(bn_00, bn_const_zero);
    bn_00.mul(bn_const_2, bn_const_m1);
    EXPECT_TRUE(bn_00.is_negative());
    EXPECT_EQ(bn_00, bn_const_m2);
}

TEST_F(BignumTestBase, TestSub) {
    bn_00.sub(bn_const_100, bn_const_m100);
    EXPECT_EQ(bn_00, bn_const_200);
    bn_00.sub(bn_const_100, bn_const_100);
    EXPECT_EQ(bn_const_zero, bn_00);
}

TEST_F(BignumTestBase, TestExp) {
    // We have to horse around with values, because
    // some are const BIGNUMS, but exp can't take const.
    bn_01 = bn_const_1;
    bn_02 = bn_const_2;
    bn_04 = bn_const_4;
    bn_00.exp(bn_01, bn_02);
    EXPECT_EQ(bn_00, bn_const_1);
    bn_00.exp(bn_02, bn_04);
    EXPECT_EQ(bn_00, bn_const_16);
}

TEST_F(BignumTestBase, TestZero) {
    EXPECT_TRUE(bn_const_zero.is_zero());
    EXPECT_TRUE(bn_const_default.is_zero());
    EXPECT_FALSE(bn_const_100.is_zero());
    EXPECT_FALSE(bn_const_m100.is_zero());
    bn_00 = bn_const_100;
    EXPECT_EQ(bn_00, bn_const_100);
    EXPECT_FALSE(bn_00.is_zero());
    bn_00.set_zero();
    EXPECT_NE(bn_00, bn_const_100);
    EXPECT_EQ(bn_const_zero, bn_00);
    EXPECT_TRUE(bn_00.is_zero());
}

TEST_F(BignumTestBase, TestGetWord) {
    EXPECT_EQ(   0, bn_const_zero.get_word());
    EXPECT_EQ( 100, bn_const_100.get_word());
    EXPECT_EQ(-100, bn_const_m100.get_word());
}

TEST_F(BignumTestBase, TestCopy) {
    BIGNUM *cp = bn_00.copy(bn_const_100);
    EXPECT_EQ(cp, &bn_00);
    EXPECT_EQ(bn_00, bn_const_100);
    cp = bn_00.copy(bn_const_zero);
    EXPECT_EQ(cp, &bn_00);
    EXPECT_EQ(*cp, bn_const_zero);
}

TEST_F(BignumTestBase, TestIsBitSet) {
    BIGNUM everyOther{0xAAAAAAAA};
    for (int idx = 0; idx < 64; idx += 1) {
        if (idx <= 32 && (idx % 2)) {
            EXPECT_TRUE(everyOther.is_bit_set(idx));
        } else {
            EXPECT_FALSE(everyOther.is_bit_set(idx));
        }
    }
}

TEST_F(BignumTestBase, TestShifts) {
    bn_00 = bn_const_1;
    for (int idx = 0; idx < BIGNUM::SIZE_IN_BITS-1; idx += 1) {
        for (int bit = 0; bit < BIGNUM::SIZE_IN_BITS-1; bit += 1) {
            EXPECT_EQ(bit == idx, bn_00.is_bit_set(bit));
        }
        bn_02 = bn_00;
        EXPECT_EQ(1, bn_00.lshift(bn_02, 1));
    }
    EXPECT_EQ(1, bn_00.lshift(bn_const_1, BIGNUM::SIZE_IN_BITS-2));
    for (int idx = BIGNUM::SIZE_IN_BITS-2; 0 <= idx; idx -= 1) {
        for (int bit = 0; bit < BIGNUM::SIZE_IN_BITS-1; bit += 1) {
            EXPECT_EQ(bit == idx, bn_00.is_bit_set(bit));
        }
        bn_02 = bn_00;
        EXPECT_EQ(1, bn_00.rshift(bn_02, 1));
    }
}

TEST_F(BignumTestBase, TestCompares) {
    EXPECT_EQ( 1, bn_const_zero.cmp(bn_const_m100));
    EXPECT_EQ( 0, bn_const_zero.cmp(bn_const_zero));
    EXPECT_EQ( 0, bn_const_m100.cmp(bn_const_m100_too));
    EXPECT_EQ( 0, bn_const_100.cmp(bn_const_100_too));
    EXPECT_EQ(-1, bn_const_m100.cmp(bn_const_zero));
    EXPECT_EQ( 1, bn_const_1.cmp(bn_const_m100));
    bn_00 = bn_const_1;
    EXPECT_EQ( 0, bn_const_1.cmp(bn_00));
    EXPECT_EQ( 0, bn_00.cmp(bn_const_1));
    EXPECT_EQ(-1, bn_const_m100.cmp(bn_const_1));

    EXPECT_EQ(-1, bn_const_zero.ucmp(bn_const_m100));
    EXPECT_EQ( 0, bn_const_zero.ucmp(bn_const_zero));
    EXPECT_EQ( 1, bn_const_m100.ucmp(bn_const_zero));
    EXPECT_EQ(-1, bn_const_1.ucmp(bn_const_m100));
    EXPECT_EQ( 0, bn_const_1.ucmp(bn_const_1));
    EXPECT_EQ( 1, bn_const_m100.ucmp(bn_const_1));
}

TEST_F(BignumTestBase, TestBits) {
    EXPECT_TRUE(bn_const_m100.is_negative());
    EXPECT_TRUE(bn_const_m200.is_negative());
    bn_00 = bn_const_100;
    EXPECT_FALSE(bn_00.is_negative());
    bn_00.set_negative(0);
    EXPECT_EQ(bn_const_m100, bn_00);
    EXPECT_TRUE(bn_00.is_negative());
    bn_00.set_zero();
    EXPECT_TRUE(bn_00.is_zero());
    for (int idx = 0; idx < 100; idx += 1) {
    	EXPECT_EQ(bn_00.is_odd(), ((idx & 0x1) == 1));
    	bn_00.add(1);
    }
    bn_00 = bn_const_1;
    for (int idx = 0; idx < BIGNUM::SIZE_IN_BITS-2; idx += 1) {
    	int bits = bn_00.ext_count_low_zero_bits();
    	EXPECT_EQ(idx, bits);
    	EXPECT_EQ(idx+1, bn_00.num_bits());
    	bn_01.lshift(bn_00, 1);
    	bn_00 = bn_01;
    }
}
/*************************************************************************
 * Then we have some tests of the crypto library redirection functions.
 *************************************************************************/
TEST_F(BignumTestBase, TestAlloc) {
	BIGNUM *bnp = BN_new();
	BN_init(bnp);
	EXPECT_EQ(*bnp, bn_const_zero);
	BN_free(bnp);
	EXPECT_TRUE(true);
}

TEST_F(BignumTestBase, TestString) {
	const char *str = BN_bn2dec(&bn_const_zero);
	EXPECT_STREQ("0", str);
	OPENSSL_free(str);
	str = BN_bn2dec(&bn_const_1);
	EXPECT_STREQ("1", str);
	OPENSSL_free(str);
	str = BN_bn2dec(&bn_const_m100);
	EXPECT_STREQ("-100", str);
	OPENSSL_free(str);
}

TEST_F(BignumTestBase, TextBNContext) {
	// Test contexts.
	BN_CTX *ctx = BN_CTX_new();
	EXPECT_NE(nullptr, ctx);
	BN_CTX_free(ctx);
	EXPECT_TRUE(true);
}

TEST_F(BignumTestBase, TestArith) {
	// Needed below.  Vestigial.
	BN_CTX *ctx = BN_CTX_new();

	EXPECT_EQ(1, BN_add(&bn_00, &bn_const_100, &bn_const_200));
	EXPECT_EQ(bn_00, bn_const_300);
	EXPECT_EQ(1, BN_add(&bn_00, &bn_const_100, &bn_const_m100));
	EXPECT_EQ(bn_const_zero, bn_00);
	EXPECT_TRUE(bn_00.is_zero());
	EXPECT_FALSE(bn_00.is_negative());

	EXPECT_EQ(1, BN_zero(&bn_00));
	EXPECT_TRUE(bn_00.is_zero());
	EXPECT_TRUE(BN_is_zero(&bn_00));
	EXPECT_EQ(1, BN_add_word(&bn_00, 100));
	EXPECT_EQ(bn_00, bn_const_100);

	EXPECT_EQ(1, BN_sub(&bn_00, &bn_const_200, &bn_const_100));
	EXPECT_EQ(bn_00, bn_const_100);

	// Horse around with non-const values because
	// BN_exp can't take const objects.
	bn_02 = bn_const_2;
	bn_04 = bn_const_4;
	EXPECT_EQ(1, BN_exp(&bn_00, &bn_02, &bn_04, ctx));
	EXPECT_EQ(bn_00, bn_const_16);

	EXPECT_EQ(1, BN_mul(&bn_00, &bn_const_2, &bn_const_4, ctx));
	EXPECT_EQ(bn_00, bn_const_8);

	EXPECT_EQ(1, BN_mul(&bn_00, &bn_const_zero, &bn_const_4, ctx));
	EXPECT_EQ(bn_const_zero, bn_00);

	EXPECT_TRUE(BN_is_odd(&bn_const_1));
	EXPECT_FALSE(BN_is_odd(&bn_const_16));

	EXPECT_EQ(100, BN_get_word(&bn_const_100));
	EXPECT_EQ(-100, BN_get_word(&bn_const_m100));

	// Shift a 1 up.  Verify that the shifted number has
	// the right number of bits.
	bn_00 = bn_const_1;
	for (int idx = 0; idx < BIGNUM::SIZE_IN_BITS-2; idx += 1) {
		EXPECT_EQ(idx+1, BN_num_bits(&bn_00));
		EXPECT_EQ(1, BN_lshift(&bn_01, &bn_00, 1));
		BIGNUM *bp = BN_copy(&bn_00, &bn_01);
		EXPECT_EQ(bp, &bn_00);
	}
	// Now, shift the 1 back down.  Verify that it has the right
	// number of bits again.
	for (int idx = BIGNUM::SIZE_IN_BITS-2; 0 <= idx; idx -= 1) {
		EXPECT_EQ(idx+1, BN_num_bits(&bn_00));
		EXPECT_EQ(1, BN_rshift(&bn_01, &bn_00, 1));
		bn_00 = bn_01;
	}
}
