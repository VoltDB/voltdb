/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google_voltpatches.common.math;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.math.MathPreconditions.checkNoOverflow;
import static com.google_voltpatches.common.math.MathPreconditions.checkNonNegative;
import static com.google_voltpatches.common.math.MathPreconditions.checkPositive;
import static com.google_voltpatches.common.math.MathPreconditions.checkRoundingUnnecessary;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.math.RoundingMode.HALF_EVEN;
import static java.math.RoundingMode.HALF_UP;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.common.annotations.GwtIncompatible;
import com.google_voltpatches.common.annotations.VisibleForTesting;

import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * A class for arithmetic on values of type {@code long}. Where possible, methods are defined and
 * named analogously to their {@code BigInteger} counterparts.
 *
 * <p>The implementations of many methods in this class are based on material from Henry S. Warren,
 * Jr.'s <i>Hacker's Delight</i>, (Addison Wesley, 2002).
 *
 * <p>Similar functionality for {@code int} and for {@link BigInteger} can be found in
 * {@link IntMath} and {@link BigIntegerMath} respectively.  For other common operations on
 * {@code long} values, see {@link com.google_voltpatches.common.primitives.Longs}.
 *
 * @author Louis Wasserman
 * @since 11.0
 */
@GwtCompatible(emulated = true)
public final class LongMath {
  // NOTE: Whenever both tests are cheap and functional, it's faster to use &, | instead of &&, ||

  /**
   * Returns {@code true} if {@code x} represents a power of two.
   *
   * <p>This differs from {@code Long.bitCount(x) == 1}, because
   * {@code Long.bitCount(Long.MIN_VALUE) == 1}, but {@link Long#MIN_VALUE} is not a power of two.
   */
  public static boolean isPowerOfTwo(long x) {
    return x > 0 & (x & (x - 1)) == 0;
  }

  /**
   * Returns 1 if {@code x < y} as unsigned longs, and 0 otherwise.  Assumes that x - y fits into a
   * signed long.  The implementation is branch-free, and benchmarks suggest it is measurably
   * faster than the straightforward ternary expression.
   */
  @VisibleForTesting
  static int lessThanBranchFree(long x, long y) {
    // Returns the sign bit of x - y.
    return (int) (~~(x - y) >>> (Long.SIZE - 1));
  }

  /**
   * Returns the base-2 logarithm of {@code x}, rounded according to the specified rounding mode.
   *
   * @throws IllegalArgumentException if {@code x <= 0}
   * @throws ArithmeticException if {@code mode} is {@link RoundingMode#UNNECESSARY} and {@code x}
   *         is not a power of two
   */
  @SuppressWarnings("fallthrough")
  // TODO(kevinb): remove after this warning is disabled globally
  public static int log2(long x, RoundingMode mode) {
    checkPositive("x", x);
    switch (mode) {
      case UNNECESSARY:
        checkRoundingUnnecessary(isPowerOfTwo(x));
        // fall through
      case DOWN:
      case FLOOR:
        return (Long.SIZE - 1) - Long.numberOfLeadingZeros(x);

      case UP:
      case CEILING:
        return Long.SIZE - Long.numberOfLeadingZeros(x - 1);

      case HALF_DOWN:
      case HALF_UP:
      case HALF_EVEN:
        // Since sqrt(2) is irrational, log2(x) - logFloor cannot be exactly 0.5
        int leadingZeros = Long.numberOfLeadingZeros(x);
        long cmp = MAX_POWER_OF_SQRT2_UNSIGNED >>> leadingZeros;
        // floor(2^(logFloor + 0.5))
        int logFloor = (Long.SIZE - 1) - leadingZeros;
        return logFloor + lessThanBranchFree(cmp, x);

      default:
        throw new AssertionError("impossible");
    }
  }

  /** The biggest half power of two that fits into an unsigned long */
  @VisibleForTesting static final long MAX_POWER_OF_SQRT2_UNSIGNED = 0xB504F333F9DE6484L;

  /**
   * Returns the base-10 logarithm of {@code x}, rounded according to the specified rounding mode.
   *
   * @throws IllegalArgumentException if {@code x <= 0}
   * @throws ArithmeticException if {@code mode} is {@link RoundingMode#UNNECESSARY} and {@code x}
   *         is not a power of ten
   */
  @GwtIncompatible("TODO")
  @SuppressWarnings("fallthrough")
  // TODO(kevinb): remove after this warning is disabled globally
  public static int log10(long x, RoundingMode mode) {
    checkPositive("x", x);
    int logFloor = log10Floor(x);
    long floorPow = powersOf10[logFloor];
    switch (mode) {
      case UNNECESSARY:
        checkRoundingUnnecessary(x == floorPow);
        // fall through
      case FLOOR:
      case DOWN:
        return logFloor;
      case CEILING:
      case UP:
        return logFloor + lessThanBranchFree(floorPow, x);
      case HALF_DOWN:
      case HALF_UP:
      case HALF_EVEN:
        // sqrt(10) is irrational, so log10(x)-logFloor is never exactly 0.5
        return logFloor + lessThanBranchFree(halfPowersOf10[logFloor], x);
      default:
        throw new AssertionError();
    }
  }

  @GwtIncompatible("TODO")
  static int log10Floor(long x) {
    /*
     * Based on Hacker's Delight Fig. 11-5, the two-table-lookup, branch-free implementation.
     *
     * The key idea is that based on the number of leading zeros (equivalently, floor(log2(x))),
     * we can narrow the possible floor(log10(x)) values to two.  For example, if floor(log2(x))
     * is 6, then 64 <= x < 128, so floor(log10(x)) is either 1 or 2.
     */
    int y = maxLog10ForLeadingZeros[Long.numberOfLeadingZeros(x)];
    /*
     * y is the higher of the two possible values of floor(log10(x)). If x < 10^y, then we want the
     * lower of the two possible values, or y - 1, otherwise, we want y.
     */
    return y - lessThanBranchFree(x, powersOf10[y]);
  }

  // maxLog10ForLeadingZeros[i] == floor(log10(2^(Long.SIZE - i)))
  @VisibleForTesting static final byte[] maxLog10ForLeadingZeros = {
      19, 18, 18, 18, 18, 17, 17, 17, 16, 16, 16, 15, 15, 15, 15, 14, 14, 14, 13, 13, 13, 12, 12,
      12, 12, 11, 11, 11, 10, 10, 10, 9, 9, 9, 9, 8, 8, 8, 7, 7, 7, 6, 6, 6, 6, 5, 5, 5, 4, 4, 4,
      3, 3, 3, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0 };

  @GwtIncompatible("TODO")
  @VisibleForTesting
  static final long[] powersOf10 = {
    1L,
    10L,
    100L,
    1000L,
    10000L,
    100000L,
    1000000L,
    10000000L,
    100000000L,
    1000000000L,
    10000000000L,
    100000000000L,
    1000000000000L,
    10000000000000L,
    100000000000000L,
    1000000000000000L,
    10000000000000000L,
    100000000000000000L,
    1000000000000000000L
  };

  // halfPowersOf10[i] = largest long less than 10^(i + 0.5)
  @GwtIncompatible("TODO")
  @VisibleForTesting
  static final long[] halfPowersOf10 = {
    3L,
    31L,
    316L,
    3162L,
    31622L,
    316227L,
    3162277L,
    31622776L,
    316227766L,
    3162277660L,
    31622776601L,
    316227766016L,
    3162277660168L,
    31622776601683L,
    316227766016837L,
    3162277660168379L,
    31622776601683793L,
    316227766016837933L,
    3162277660168379331L
  };

  /**
   * Returns {@code b} to the {@code k}th power. Even if the result overflows, it will be equal to
   * {@code BigInteger.valueOf(b).pow(k).longValue()}. This implementation runs in {@code O(log k)}
   * time.
   *
   * @throws IllegalArgumentException if {@code k < 0}
   */
  @GwtIncompatible("TODO")
  public static long pow(long b, int k) {
    checkNonNegative("exponent", k);
    if (-2 <= b && b <= 2) {
      switch ((int) b) {
        case 0:
          return (k == 0) ? 1 : 0;
        case 1:
          return 1;
        case (-1):
          return ((k & 1) == 0) ? 1 : -1;
        case 2:
          return (k < Long.SIZE) ? 1L << k : 0;
        case (-2):
          if (k < Long.SIZE) {
            return ((k & 1) == 0) ? 1L << k : -(1L << k);
          } else {
            return 0;
          }
        default:
          throw new AssertionError();
      }
    }
    for (long accum = 1;; k >>= 1) {
      switch (k) {
        case 0:
          return accum;
        case 1:
          return accum * b;
        default:
          accum *= ((k & 1) == 0) ? 1 : b;
          b *= b;
      }
    }
  }

  /**
   * Returns the square root of {@code x}, rounded with the specified rounding mode.
   *
   * @throws IllegalArgumentException if {@code x < 0}
   * @throws ArithmeticException if {@code mode} is {@link RoundingMode#UNNECESSARY} and
   *         {@code sqrt(x)} is not an integer
   */
  @GwtIncompatible("TODO")
  @SuppressWarnings("fallthrough")
  public static long sqrt(long x, RoundingMode mode) {
    checkNonNegative("x", x);
    if (fitsInInt(x)) {
      return IntMath.sqrt((int) x, mode);
    }
    /*
     * Let k be the true value of floor(sqrt(x)), so that
     *
     *            k * k <= x          <  (k + 1) * (k + 1)
     * (double) (k * k) <= (double) x <= (double) ((k + 1) * (k + 1))
     *          since casting to double is nondecreasing.
     *          Note that the right-hand inequality is no longer strict.
     * Math.sqrt(k * k) <= Math.sqrt(x) <= Math.sqrt((k + 1) * (k + 1))
     *          since Math.sqrt is monotonic.
     * (long) Math.sqrt(k * k) <= (long) Math.sqrt(x) <= (long) Math.sqrt((k + 1) * (k + 1))
     *          since casting to long is monotonic
     * k <= (long) Math.sqrt(x) <= k + 1
     *          since (long) Math.sqrt(k * k) == k, as checked exhaustively in
     *          {@link LongMathTest#testSqrtOfPerfectSquareAsDoubleIsPerfect}
     */
    long guess = (long) Math.sqrt(x);
    // Note: guess is always <= FLOOR_SQRT_MAX_LONG.
    long guessSquared = guess * guess;
    // Note (2013-2-26): benchmarks indicate that, inscrutably enough, using if statements is
    // faster here than using lessThanBranchFree.
    switch (mode) {
      case UNNECESSARY:
        checkRoundingUnnecessary(guessSquared == x);
        return guess;
      case FLOOR:
      case DOWN:
        if (x < guessSquared) {
          return guess - 1;
        }
        return guess;
      case CEILING:
      case UP:
        if (x > guessSquared) {
          return guess + 1;
        }
        return guess;
      case HALF_DOWN:
      case HALF_UP:
      case HALF_EVEN:
        long sqrtFloor = guess - ((x < guessSquared) ? 1 : 0);
        long halfSquare = sqrtFloor * sqrtFloor + sqrtFloor;
        /*
         * We wish to test whether or not x <= (sqrtFloor + 0.5)^2 = halfSquare + 0.25. Since both
         * x and halfSquare are integers, this is equivalent to testing whether or not x <=
         * halfSquare. (We have to deal with overflow, though.)
         *
         * If we treat halfSquare as an unsigned long, we know that
         *            sqrtFloor^2 <= x < (sqrtFloor + 1)^2
         * halfSquare - sqrtFloor <= x < halfSquare + sqrtFloor + 1
         * so |x - halfSquare| <= sqrtFloor.  Therefore, it's safe to treat x - halfSquare as a
         * signed long, so lessThanBranchFree is safe for use.
         */
        return sqrtFloor + lessThanBranchFree(halfSquare, x);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns the result of dividing {@code p} by {@code q}, rounding using the specified
   * {@code RoundingMode}.
   *
   * @throws ArithmeticException if {@code q == 0}, or if {@code mode == UNNECESSARY} and {@code a}
   *         is not an integer multiple of {@code b}
   */
  @GwtIncompatible("TODO")
  @SuppressWarnings("fallthrough")
  public static long divide(long p, long q, RoundingMode mode) {
    checkNotNull(mode);
    long div = p / q; // throws if q == 0
    long rem = p - q * div; // equals p % q

    if (rem == 0) {
      return div;
    }

    /*
     * Normal Java division rounds towards 0, consistently with RoundingMode.DOWN. We just have to
     * deal with the cases where rounding towards 0 is wrong, which typically depends on the sign of
     * p / q.
     *
     * signum is 1 if p and q are both nonnegative or both negative, and -1 otherwise.
     */
    int signum = 1 | (int) ((p ^ q) >> (Long.SIZE - 1));
    boolean increment;
    switch (mode) {
      case UNNECESSARY:
        checkRoundingUnnecessary(rem == 0);
        // fall through
      case DOWN:
        increment = false;
        break;
      case UP:
        increment = true;
        break;
      case CEILING:
        increment = signum > 0;
        break;
      case FLOOR:
        increment = signum < 0;
        break;
      case HALF_EVEN:
      case HALF_DOWN:
      case HALF_UP:
        long absRem = abs(rem);
        long cmpRemToHalfDivisor = absRem - (abs(q) - absRem);
        // subtracting two nonnegative longs can't overflow
        // cmpRemToHalfDivisor has the same sign as compare(abs(rem), abs(q) / 2).
        if (cmpRemToHalfDivisor == 0) { // exactly on the half mark
          increment = (mode == HALF_UP | (mode == HALF_EVEN & (div & 1) != 0));
        } else {
          increment = cmpRemToHalfDivisor > 0; // closer to the UP value
        }
        break;
      default:
        throw new AssertionError();
    }
    return increment ? div + signum : div;
  }

  /**
   * Returns {@code x mod m}, a non-negative value less than {@code m}.
   * This differs from {@code x % m}, which might be negative.
   *
   * <p>For example:
   *
   * <pre> {@code
   *
   * mod(7, 4) == 3
   * mod(-7, 4) == 1
   * mod(-1, 4) == 3
   * mod(-8, 4) == 0
   * mod(8, 4) == 0}</pre>
   *
   * @throws ArithmeticException if {@code m <= 0}
   * @see <a href="http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.17.3">
   *      Remainder Operator</a>
   */
  @GwtIncompatible("TODO")
  public static int mod(long x, int m) {
    // Cast is safe because the result is guaranteed in the range [0, m)
    return (int) mod(x, (long) m);
  }

  /**
   * Returns {@code x mod m}, a non-negative value less than {@code m}.
   * This differs from {@code x % m}, which might be negative.
   *
   * <p>For example:
   *
   * <pre> {@code
   *
   * mod(7, 4) == 3
   * mod(-7, 4) == 1
   * mod(-1, 4) == 3
   * mod(-8, 4) == 0
   * mod(8, 4) == 0}</pre>
   *
   * @throws ArithmeticException if {@code m <= 0}
   * @see <a href="http://docs.oracle.com/javase/specs/jls/se7/html/jls-15.html#jls-15.17.3">
   *      Remainder Operator</a>
   */
  @GwtIncompatible("TODO")
  public static long mod(long x, long m) {
    if (m <= 0) {
      throw new ArithmeticException("Modulus must be positive");
    }
    long result = x % m;
    return (result >= 0) ? result : result + m;
  }

  /**
   * Returns the greatest common divisor of {@code a, b}. Returns {@code 0} if
   * {@code a == 0 && b == 0}.
   *
   * @throws IllegalArgumentException if {@code a < 0} or {@code b < 0}
   */
  public static long gcd(long a, long b) {
    /*
     * The reason we require both arguments to be >= 0 is because otherwise, what do you return on
     * gcd(0, Long.MIN_VALUE)? BigInteger.gcd would return positive 2^63, but positive 2^63 isn't
     * an int.
     */
    checkNonNegative("a", a);
    checkNonNegative("b", b);
    if (a == 0) {
      // 0 % b == 0, so b divides a, but the converse doesn't hold.
      // BigInteger.gcd is consistent with this decision.
      return b;
    } else if (b == 0) {
      return a; // similar logic
    }
    /*
     * Uses the binary GCD algorithm; see http://en.wikipedia.org/wiki/Binary_GCD_algorithm.
     * This is >60% faster than the Euclidean algorithm in benchmarks.
     */
    int aTwos = Long.numberOfTrailingZeros(a);
    a >>= aTwos; // divide out all 2s
    int bTwos = Long.numberOfTrailingZeros(b);
    b >>= bTwos; // divide out all 2s
    while (a != b) { // both a, b are odd
      // The key to the binary GCD algorithm is as follows:
      // Both a and b are odd.  Assume a > b; then gcd(a - b, b) = gcd(a, b).
      // But in gcd(a - b, b), a - b is even and b is odd, so we can divide out powers of two.

      // We bend over backwards to avoid branching, adapting a technique from
      // http://graphics.stanford.edu/~seander/bithacks.html#IntegerMinOrMax

      long delta = a - b; // can't overflow, since a and b are nonnegative

      long minDeltaOrZero = delta & (delta >> (Long.SIZE - 1));
      // equivalent to Math.min(delta, 0)

      a = delta - minDeltaOrZero - minDeltaOrZero; // sets a to Math.abs(a - b)
      // a is now nonnegative and even

      b += minDeltaOrZero; // sets b to min(old a, b)
      a >>= Long.numberOfTrailingZeros(a); // divide out all 2s, since 2 doesn't divide b
    }
    return a << min(aTwos, bTwos);
  }

  /**
   * Returns the sum of {@code a} and {@code b}, provided it does not overflow.
   *
   * @throws ArithmeticException if {@code a + b} overflows in signed {@code long} arithmetic
   */
  @GwtIncompatible("TODO")
  public static long checkedAdd(long a, long b) {
    long result = a + b;
    checkNoOverflow((a ^ b) < 0 | (a ^ result) >= 0);
    return result;
  }

  /**
   * Returns the difference of {@code a} and {@code b}, provided it does not overflow.
   *
   * @throws ArithmeticException if {@code a - b} overflows in signed {@code long} arithmetic
   */
  @GwtIncompatible("TODO")
  public static long checkedSubtract(long a, long b) {
    long result = a - b;
    checkNoOverflow((a ^ b) >= 0 | (a ^ result) >= 0);
    return result;
  }

  /**
   * Returns the product of {@code a} and {@code b}, provided it does not overflow.
   *
   * @throws ArithmeticException if {@code a * b} overflows in signed {@code long} arithmetic
   */
  @GwtIncompatible("TODO")
  public static long checkedMultiply(long a, long b) {
    // Hacker's Delight, Section 2-12
    int leadingZeros = Long.numberOfLeadingZeros(a) + Long.numberOfLeadingZeros(~a)
        + Long.numberOfLeadingZeros(b) + Long.numberOfLeadingZeros(~b);
    /*
     * If leadingZeros > Long.SIZE + 1 it's definitely fine, if it's < Long.SIZE it's definitely
     * bad. We do the leadingZeros check to avoid the division below if at all possible.
     *
     * Otherwise, if b == Long.MIN_VALUE, then the only allowed values of a are 0 and 1. We take
     * care of all a < 0 with their own check, because in particular, the case a == -1 will
     * incorrectly pass the division check below.
     *
     * In all other cases, we check that either a is 0 or the result is consistent with division.
     */
    if (leadingZeros > Long.SIZE + 1) {
      return a * b;
    }
    checkNoOverflow(leadingZeros >= Long.SIZE);
    checkNoOverflow(a >= 0 | b != Long.MIN_VALUE);
    long result = a * b;
    checkNoOverflow(a == 0 || result / a == b);
    return result;
  }

  /**
   * Returns the {@code b} to the {@code k}th power, provided it does not overflow.
   *
   * @throws ArithmeticException if {@code b} to the {@code k}th power overflows in signed
   *         {@code long} arithmetic
   */
  @GwtIncompatible("TODO")
  public static long checkedPow(long b, int k) {
    checkNonNegative("exponent", k);
    if (b >= -2 & b <= 2) {
      switch ((int) b) {
        case 0:
          return (k == 0) ? 1 : 0;
        case 1:
          return 1;
        case (-1):
          return ((k & 1) == 0) ? 1 : -1;
        case 2:
          checkNoOverflow(k < Long.SIZE - 1);
          return 1L << k;
        case (-2):
          checkNoOverflow(k < Long.SIZE);
          return ((k & 1) == 0) ? (1L << k) : (-1L << k);
        default:
          throw new AssertionError();
      }
    }
    long accum = 1;
    while (true) {
      switch (k) {
        case 0:
          return accum;
        case 1:
          return checkedMultiply(accum, b);
        default:
          if ((k & 1) != 0) {
            accum = checkedMultiply(accum, b);
          }
          k >>= 1;
          if (k > 0) {
            checkNoOverflow(b <= FLOOR_SQRT_MAX_LONG);
            b *= b;
          }
      }
    }
  }

  @VisibleForTesting static final long FLOOR_SQRT_MAX_LONG = 3037000499L;

  /**
   * Returns {@code n!}, that is, the product of the first {@code n} positive
   * integers, {@code 1} if {@code n == 0}, or {@link Long#MAX_VALUE} if the
   * result does not fit in a {@code long}.
   *
   * @throws IllegalArgumentException if {@code n < 0}
   */
  @GwtIncompatible("TODO")
  public static long factorial(int n) {
    checkNonNegative("n", n);
    return (n < factorials.length) ? factorials[n] : Long.MAX_VALUE;
  }

  static final long[] factorials = {
      1L,
      1L,
      1L * 2,
      1L * 2 * 3,
      1L * 2 * 3 * 4,
      1L * 2 * 3 * 4 * 5,
      1L * 2 * 3 * 4 * 5 * 6,
      1L * 2 * 3 * 4 * 5 * 6 * 7,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14 * 15,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14 * 15 * 16,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14 * 15 * 16 * 17,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14 * 15 * 16 * 17 * 18,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14 * 15 * 16 * 17 * 18 * 19,
      1L * 2 * 3 * 4 * 5 * 6 * 7 * 8 * 9 * 10 * 11 * 12 * 13 * 14 * 15 * 16 * 17 * 18 * 19 * 20
  };

  /**
   * Returns {@code n} choose {@code k}, also known as the binomial coefficient of {@code n} and
   * {@code k}, or {@link Long#MAX_VALUE} if the result does not fit in a {@code long}.
   *
   * @throws IllegalArgumentException if {@code n < 0}, {@code k < 0}, or {@code k > n}
   */
  public static long binomial(int n, int k) {
    checkNonNegative("n", n);
    checkNonNegative("k", k);
    checkArgument(k <= n, "k (%s) > n (%s)", k, n);
    if (k > (n >> 1)) {
      k = n - k;
    }
    switch (k) {
      case 0:
        return 1;
      case 1:
        return n;
      default:
        if (n < factorials.length) {
          return factorials[n] / (factorials[k] * factorials[n - k]);
        } else if (k >= biggestBinomials.length || n > biggestBinomials[k]) {
          return Long.MAX_VALUE;
        } else if (k < biggestSimpleBinomials.length && n <= biggestSimpleBinomials[k]) {
          // guaranteed not to overflow
          long result = n--;
          for (int i = 2; i <= k; n--, i++) {
            result *= n;
            result /= i;
          }
          return result;
        } else {
          int nBits = LongMath.log2(n, RoundingMode.CEILING);

          long result = 1;
          long numerator = n--;
          long denominator = 1;

          int numeratorBits = nBits;
          // This is an upper bound on log2(numerator, ceiling).

          /*
           * We want to do this in long math for speed, but want to avoid overflow. We adapt the
           * technique previously used by BigIntegerMath: maintain separate numerator and
           * denominator accumulators, multiplying the fraction into result when near overflow.
           */
          for (int i = 2; i <= k; i++, n--) {
            if (numeratorBits + nBits < Long.SIZE - 1) {
              // It's definitely safe to multiply into numerator and denominator.
              numerator *= n;
              denominator *= i;
              numeratorBits += nBits;
            } else {
              // It might not be safe to multiply into numerator and denominator,
              // so multiply (numerator / denominator) into result.
              result = multiplyFraction(result, numerator, denominator);
              numerator = n;
              denominator = i;
              numeratorBits = nBits;
            }
          }
          return multiplyFraction(result, numerator, denominator);
        }
    }
  }

  /**
   * Returns (x * numerator / denominator), which is assumed to come out to an integral value.
   */
  static long multiplyFraction(long x, long numerator, long denominator) {
    if (x == 1) {
      return numerator / denominator;
    }
    long commonDivisor = gcd(x, denominator);
    x /= commonDivisor;
    denominator /= commonDivisor;
    // We know gcd(x, denominator) = 1, and x * numerator / denominator is exact,
    // so denominator must be a divisor of numerator.
    return x * (numerator / denominator);
  }

  /*
   * binomial(biggestBinomials[k], k) fits in a long, but not
   * binomial(biggestBinomials[k] + 1, k).
   */
  static final int[] biggestBinomials =
      {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, 3810779, 121977, 16175, 4337, 1733,
          887, 534, 361, 265, 206, 169, 143, 125, 111, 101, 94, 88, 83, 79, 76, 74, 72, 70, 69, 68,
          67, 67, 66, 66, 66, 66};

  /*
   * binomial(biggestSimpleBinomials[k], k) doesn't need to use the slower GCD-based impl,
   * but binomial(biggestSimpleBinomials[k] + 1, k) does.
   */
  @VisibleForTesting static final int[] biggestSimpleBinomials =
      {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, 2642246, 86251, 11724, 3218, 1313,
          684, 419, 287, 214, 169, 139, 119, 105, 95, 87, 81, 76, 73, 70, 68, 66, 64, 63, 62, 62,
          61, 61, 61};
  // These values were generated by using checkedMultiply to see when the simple multiply/divide
  // algorithm would lead to an overflow.

  static boolean fitsInInt(long x) {
    return (int) x == x;
  }

  /**
   * Returns the arithmetic mean of {@code x} and {@code y}, rounded toward
   * negative infinity. This method is resilient to overflow.
   *
   * @since 14.0
   */
  public static long mean(long x, long y) {
    // Efficient method for computing the arithmetic mean.
    // The alternative (x + y) / 2 fails for large values.
    // The alternative (x + y) >>> 1 fails for negative values.
    return (x & y) + ((x ^ y) >> 1);
  }

  private LongMath() {}
}
