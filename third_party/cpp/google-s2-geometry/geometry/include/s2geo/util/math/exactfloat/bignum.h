/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef COMMON_BIGNUM_H_
#define COMMON_BIGNUM_H_
/*
 * This file contains definitions of the arbitrary arithmetic
 * operations from libcrypto which are used by the S2 library.
 * It is not a complete or comprehensive arbitrary precision
 * arithmetic library.  It's just what we need to fill a gap.
 * Supporting libcrypto proved to be more work than reimplementing
 * these.
 *
 * Note that these functions are implemented in terms of the
 * ttmath library.
 */
#include <ttmath/ttmath.h>
/************************************************************************
 * Types
 ************************************************************************/
/**
 *  The type BN_ULONG is the type of unsigned longs.
 *  The type BN_SLONG is the type of signed longs.
 */
typedef ttmath::uint BN_ULONG;
typedef ttmath::sint BN_SLONG;

/**
 * A BN_CTX contains variables needed for BIGNUM values.
 */

#define BN_BITS2 64

class BN_CTX {
 public:
    static BN_CTX *create() {
        return new BN_CTX;
    }
    void destroy() {
        delete this;
    }
};

/**
 * A BIGNUM is an arbitrary precision integer.
 */
struct BIGNUM {
    /**
     * This is the number of bits in a word.  This could be
     * defined somewhere else, and depends on the platform.
     */
    static unsigned int constexpr 	WORD_SIZE = 64;
    /**
     * This is the number of words in a BIGNUM.
     */
    static unsigned int constexpr 	SIZE_IN_WORDS      = 4;
    /**
     * This is the number of bits in a BIGNUM.
     */
    static unsigned int constexpr 	SIZE_IN_BITS  = WORD_SIZE * SIZE_IN_WORDS;
    /**
     * This is the default value for a BIGNUM.
     */
    static unsigned int constexpr 	DEFAULT_INITIAL_VALUE = 0;
    /**
     * This is the default numeric radix for toString.
     */
    static int constexpr 			DEFAULT_RADIX = 10;
 protected:
    /**
     * This is the type of the internal data structure.
     */
    typedef ttmath::Int<SIZE_IN_WORDS>   value_type;
    typedef ttmath::UInt<SIZE_IN_WORDS> uvalue_type;
    /**
     * Return the internal structure, in const and non-const.
     */
    inline const value_type &get_value() const {
        return big_value;
    }
    inline       value_type &get_value()       {
        return big_value;
    }

 public:
    inline BIGNUM(BN_SLONG w = DEFAULT_INITIAL_VALUE) {
        init(w);
    }
    inline bool operator==(const BIGNUM &other) const {
    	return big_value == other.get_value();
    }
    inline bool operator!=(const BIGNUM &other) const {
    	return big_value != other.get_value();
    }
    /**
     * Factory method to create a new BIGNUM.
     */
    inline static BIGNUM *create(BN_SLONG w = DEFAULT_INITIAL_VALUE) {
        BIGNUM *answer = new BIGNUM();
        answer->init(w);
        return answer;
    }

    /**
     * Destroy this BIGNUM.
     */
    inline void destroy() {
        delete this;
    }

    /**
     * Initialize this bignum.
     */
    inline void init(BN_SLONG w = DEFAULT_INITIAL_VALUE) {
        big_value = w;
    }

    inline const char *toString(unsigned int base = DEFAULT_RADIX) const {
        std::string result;
        big_value.ToString(result, base);
        char *answer = new char [result.size() + 1];
        memmove(answer, result.c_str(), result.size());
        answer[result.size()] = '\0';
        return answer;
    }

    /**
     * Dispose of the string data toString returns.
     */
    inline static void disposeString(const char *p) {
        delete []p;
    }

    /**
     * Set this to a + b.
     */
    inline int add(const BIGNUM &a, const BIGNUM &b) {
        big_value = a.get_value() + b.get_value();
        return 1;
    }

    /**
     * Set this to this + w.
     */
    inline int add(BN_SLONG w) {
        big_value += value_type(w);
        return 1;
    }

    /**
     * Set this to a - b.
     */
    inline int sub(const BIGNUM &a, const BIGNUM &b) {
        big_value = a.get_value() - b.get_value();
        return 1;
    }

    /**
     * Set this to a * b.
     */
    inline int mul(const BIGNUM &a, const BIGNUM &b) {
        big_value = a.get_value() * b.get_value();
        return 1;
    }

    /**
     * Set this to a ^ p.  Note that this is all integer
     * arithmetic.
     */
    inline int exp(BIGNUM &a, BIGNUM &p) {
    	big_value = a.get_value();
        int retstat = get_value().Pow(p.get_value());
        return retstat == 0;
    }

    inline bool is_zero() const {
        return get_value() == 0;
    }

    inline int set_zero() {
        get_value().SetZero();
        return 1;
    }

    inline int set_word(BN_SLONG w) {
        get_value() = w;
        return 1;
    }

    inline long get_word() const {
        if (big_value > value_type((uint)LONG_MAX)) {
            return 0xFFFFFFFF;
        }
        return big_value.ToInt();
    }

    inline BIGNUM *copy(const BIGNUM &a) {
        big_value = a.get_value();
        return this;
    }

    inline bool is_bit_set(int n) const {
        if (n <= BIGNUM::SIZE_IN_BITS) {
            uvalue_type one{0x1U};
            one.Rcl(n);
            uvalue_type ubv{big_value};
            ubv.BitAnd(one);
            return ubv != 0;
        } else {
            return 0;
        }

    }

    inline int lshift(const BIGNUM &a, int n) {
    	big_value = a.get_value();
        big_value.Rcl(n);
        return 1;
    }

    inline int rshift(const BIGNUM &a, int n) {
    	big_value = a.get_value();
        big_value.Rcr(n);
        return 1;
    }

    inline int ucmp(const BIGNUM &b) const {
    	value_type absl = get_value();
    	value_type absr = b.get_value();
    	if (absl < 0) {
    		absl = absl * -1;
    	}
    	if (absr < 0) {
    		absr = absr * -1;
    	}
        if (absl < absr) {
            return -1;
        } else if (absl > absr) {
            return 1;
        } else {
            return 0;
        }
    }

    inline int cmp(const BIGNUM &b) const {
    	value_type vtl = get_value();
    	value_type vtr = b.get_value();
        if (vtl < vtr) {
            return -1;
        } else if (vtl > vtr) {
            return 1;
        } else {
            return 0;
        }
    }

    inline int is_odd() const {
    	uvalue_type ubv{big_value};
    	uvalue_type one{0x1U};
        ubv.BitAnd(one);
        return ubv != 0;
    }

    inline int is_negative() const {
        if (big_value < 0) {
            return 1;
        }
        return 0;
    }
    inline int set_negative(int n) {
        if (big_value > 0) {
            return big_value.ChangeSign();
        }
        return 1;
    }

    inline int ext_count_low_zero_bits() const {
        int count = 0;
        if (big_value == 0) {
            return 0;
        }
        int bidx;
        uvalue_type bit{0x1u};
        for (bidx = 0; bidx < SIZE_IN_BITS; bidx += 1) {
        	uvalue_type ubv{big_value};
        	ubv.BitAnd(bit);
            if (ubv == 0) {
                count += 1;
                bit.Rcl(1);
            } else {
                break;
            }
        }
        return count;
    }

    /**
     * Return the minimum number of bits required to represent
     * this value.
     */
    inline int num_bits() const {
        /*
         * Skip the sign bit.
         */
        uvalue_type bit;
        bit.SetOne();
        bit.Rcl(SIZE_IN_BITS-2);
        for (int bidx = SIZE_IN_BITS-2; 0 <= bidx; bidx -= 1) {
        	uvalue_type ubv{big_value};
        	ubv.BitAnd(bit);
            if (ubv != 0) {
                return bidx + 1;
            }
            bit.Rcr(1);
        }
        return 0;
    }

    inline int num_bytes() const {
        return (num_bits() + 7)/8;
    }
 private:
    value_type  big_value;
};

/*************************************************************************
 * BN_Contexts
 *************************************************************************/
/**
 * Create a new context.  We don't actually use these.  So
 * we just an empty value.  I don't want to return NULL, since
 * that might be interpreted as failure.  So I return a non-zero
 * value casted to a pointer.  This is probably a bad idea.
 *
 * These are used in the S2 exactfloat code, and I am loathe to remove
 * them right now.
 */
inline BN_CTX *BN_CTX_new() {
    return reinterpret_cast<BN_CTX *>(0xDEADBEEF);
}

/**
 * Destroy a context.  Since the context has no real contents,
 * and the pointer to it is likely a not a real pointer anyway, we
 * just don't do anything here.
 */
inline void BN_CTX_free(BN_CTX *a) {
    ;
}
/*************************************************************************
 * Initialization, Allocation and Destruction.
 *************************************************************************/
/**
 * Return a new BIGNUM.
 * Initial value is unknown, probably 0.
 */
inline BIGNUM *BN_new(void) {
    return BIGNUM::create();
}

/**
 * Free the BIGNUM a;
 */
inline void BN_free(BIGNUM *a) {
    a->destroy();
}

/**
 * Destroy the contents of the BIGNUM, but not the
 * BIGNUM itself.  For us there is nothing to do.
 */
inline void BN_free_contents(BIGNUM *a) {
	;
}

/**
 * Initialize the already-allocated BIGNUM *a.
 */
inline void BN_init(BIGNUM *a) {
    a->init(0);
}

/*************************************************************************
 * Format Conversion
 *************************************************************************/
/**
 *
 *  Convert *a to a decimal string.
 *  In OpenSSL, the result must be freed with OPENSSL_free.  Since
 *  this is our own implementation, we need to manage it differently
 */
inline const char *BN_bn2dec(const BIGNUM *a) {
    unsigned int base = 10;
    return a->toString(base);
}

inline void OPENSSL_free(const void *p) {
    delete [] static_cast<char *>(const_cast<void *>(p));
}

/*************************************************************************
 * Arithmetic.
 *************************************************************************/
/**
 *  *r = *a + *b;
 * Return 1 for success, 0 for failure.
 */
inline int BN_add(BIGNUM *r, const BIGNUM *a, const BIGNUM *b) {
    return r->add(*a, *b);
}

/**
 *  *a += w;
 *  Return 1 for success, 0 for failure.
 */
inline int BN_add_word(BIGNUM *a, BN_SLONG w) {
    return a->add(w);
}

/**
 * *r = *a - *b;
 * Return 1 on success, 0 on failure.
 */
inline int BN_sub(BIGNUM *r, const BIGNUM *a, const BIGNUM *b) {
    return r->sub(*a, *b);
}

/**
 * *r = *a * *b;
 * Return 1 on success, 0 on failure.
 */
inline int BN_mul(BIGNUM *r, const BIGNUM *a, const BIGNUM *b, BN_CTX *ctx) {
    return r->mul(*a, *b);
}

/**
 *  *r = *a ^ *p;
 *  Return 1 on success, 0 on failure.
 */
inline int BN_exp(BIGNUM *r, BIGNUM *a, BIGNUM *p, BN_CTX *ctx) {
    return r->exp(*a, *p);
}

/**
 * Return 1 if *a is zero, 0 otherwise.
 */
inline int BN_is_zero(const BIGNUM *a) {
    return a->is_zero() ? 1 : 0;
}

/**
 * Set the BIGNUM *a to zero.
 */
inline int BN_zero(BIGNUM *a) {
    return a->set_zero();
}

inline int BN_is_odd(const BIGNUM *a) {
    return a->is_odd();
}

inline int BN_is_negative(const BIGNUM *a) {
    return a->is_negative();
}
/*************************************************************************
 * Structure.
 *************************************************************************/
/**
 * Copy *from to *to.
 * Return to on success, NULL on failure.
 */
inline BIGNUM *BN_copy(BIGNUM *to, const BIGNUM *from) {
    return to->copy(*from);
}

/**
 * Return *a if it can be represented as an unsigned long, and
 * 0xFFFFFFFFL if it cannot.  Since 0xFFFFFFFFL is a legal unsigned
 * long, this is problematic.
 */
inline unsigned long BN_get_word(const BIGNUM *a) {
    return a->get_word();
}

/**
 * Return 1 if bit n of *a is set.  Bit 0 is the least significant bit.
 */
inline int BN_is_bit_set(const BIGNUM *a, int n) {
    return a->is_bit_set(n);
}

/**
 * *r = (*a) * (2^n);
 * Return 1 on success, 0 on failure.
 */
inline int BN_lshift(BIGNUM *r, const BIGNUM *a, int n) {
    return r->lshift(*a, n);
}

inline int BN_rshift(BIGNUM *r, const BIGNUM *a, int n) {
    return r->rshift(*a, n);
}
/**
 *  Returns the number of significant bits in *a.
 *  If *a != 0 then this is floor(log2(*a)) + 1.
 */
inline int BN_num_bits(const BIGNUM *a) {
    return a->num_bits();
}

inline int BN_num_bytes(const BIGNUM *a) {
    return a->num_bytes();
}
/**
 *  *a = w;
 *  Return 1 on success, 0 on failure.
 */
inline int BN_set_word(BIGNUM *a, unsigned long w) {
    return a->set_word(w);
}

/**
 * Compares abs(*a) and abs(*b), where abs is the
 * absolute value.
 *
 *           -1 if abs(*a) <  abs(*b)
 *   Return   0 if abs(*a) == abs(*b)
 *            1 if abs(*a) >  abs(*b)
 *
 */
inline int BN_ucmp(const BIGNUM *a, const BIGNUM *b) {
    return a->ucmp(*b);
}

/**
 * Compares *a and *b.
 *           -1 if *a <  *b
 *   Return   0 if *a == *b
 *            1 if *a >  *b
 *
 */
inline int BN_cmp(const BIGNUM *a, const BIGNUM *b) {
    return a->cmp(*b);
}
/*************************************************************************
 * Unknown.
 *************************************************************************/
/**
 * Set the bignum *a to be negative??
 * The definition of this is not clear.
 */
inline int BN_set_negative(BIGNUM *a, int n) {
    return a->set_negative(n);
}
#endif /* COMMON_BIGNUM_H_ */
