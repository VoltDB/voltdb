/*
 * Copyright 2005 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google_voltpatches.common.geometry;


/**
 * An S1Interval represents a closed interval on a unit circle (also known as a
 * 1-dimensional sphere). It is capable of representing the empty interval
 * (containing no points), the full interval (containing all points), and
 * zero-length intervals (containing a single point).
 *
 *  Points are represented by the angle they make with the positive x-axis in
 * the range [-Pi, Pi]. An interval is represented by its lower and upper bounds
 * (both inclusive, since the interval is closed). The lower bound may be
 * greater than the upper bound, in which case the interval is "inverted" (i.e.
 * it passes through the point (-1, 0)).
 *
 *  Note that the point (-1, 0) has two valid representations, Pi and -Pi. The
 * normalized representation of this point internally is Pi, so that endpoints
 * of normal intervals are in the range (-Pi, Pi]. However, we take advantage of
 * the point -Pi to construct two special intervals: the Full() interval is
 * [-Pi, Pi], and the Empty() interval is [Pi, -Pi].
 *
 */

public final strictfp class S1Interval implements Cloneable {

  private final double lo;
  private final double hi;

  /**
   * Both endpoints must be in the range -Pi to Pi inclusive. The value -Pi is
   * converted internally to Pi except for the Full() and Empty() intervals.
   */
  public S1Interval(double lo, double hi) {
    this(lo, hi, false);
  }

  /**
   * Copy constructor. Assumes that the given interval is valid.
   *
   * TODO(dbeaumont): Make this class immutable and remove this method.
   */
  public S1Interval(S1Interval interval) {
    this.lo = interval.lo;
    this.hi = interval.hi;
  }

  /**
   * Internal constructor that assumes that both arguments are in the correct
   * range, i.e. normalization from -Pi to Pi is already done.
   */
  private S1Interval(double lo, double hi, boolean checked) {
    double newLo = lo;
    double newHi = hi;
    if (!checked) {
      if (lo == -S2.M_PI && hi != S2.M_PI) {
        newLo = S2.M_PI;
      }
      if (hi == -S2.M_PI && lo != S2.M_PI) {
        newHi = S2.M_PI;
      }
    }
    this.lo = newLo;
    this.hi = newHi;
  }

  public static S1Interval empty() {
    return new S1Interval(S2.M_PI, -S2.M_PI, true);
  }

  public static S1Interval full() {
    return new S1Interval(-S2.M_PI, S2.M_PI, true);
  }

  /** Convenience method to construct an interval containing a single point. */
  public static S1Interval fromPoint(double p) {
    if (p == -S2.M_PI) {
      p = S2.M_PI;
    }
    return new S1Interval(p, p, true);
  }

  /**
   * Convenience method to construct the minimal interval containing the two
   * given points. This is equivalent to starting with an empty interval and
   * calling AddPoint() twice, but it is more efficient.
   */
  public static S1Interval fromPointPair(double p1, double p2) {
    // assert (Math.abs(p1) <= S2.M_PI && Math.abs(p2) <= S2.M_PI);
    if (p1 == -S2.M_PI) {
      p1 = S2.M_PI;
    }
    if (p2 == -S2.M_PI) {
      p2 = S2.M_PI;
    }
    if (positiveDistance(p1, p2) <= S2.M_PI) {
      return new S1Interval(p1, p2, true);
    } else {
      return new S1Interval(p2, p1, true);
    }
  }

  public double lo() {
    return lo;
  }

  public double hi() {
    return hi;
  }

  /**
   * An interval is valid if neither bound exceeds Pi in absolute value, and the
   * value -Pi appears only in the Empty() and Full() intervals.
   */
  public boolean isValid() {
    return (Math.abs(lo()) <= S2.M_PI && Math.abs(hi()) <= S2.M_PI
        && !(lo() == -S2.M_PI && hi() != S2.M_PI) && !(hi() == -S2.M_PI && lo() != S2.M_PI));
  }

  /** Return true if the interval contains all points on the unit circle. */
  public boolean isFull() {
    return hi() - lo() == 2 * S2.M_PI;
  }


  /** Return true if the interval is empty, i.e. it contains no points. */
  public boolean isEmpty() {
    return lo() - hi() == 2 * S2.M_PI;
  }


  /* Return true if lo() > hi(). (This is true for empty intervals.) */
  public boolean isInverted() {
    return lo() > hi();
  }

  /**
   * Return the midpoint of the interval. For full and empty intervals, the
   * result is arbitrary.
   */
  public double getCenter() {
    double center = 0.5 * (lo() + hi());
    if (!isInverted()) {
      return center;
    }
    // Return the center in the range (-Pi, Pi].
    return (center <= 0) ? (center + S2.M_PI) : (center - S2.M_PI);
  }

  /**
   * Return the length of the interval. The length of an empty interval is
   * negative.
   */
  public double getLength() {
    double length = hi() - lo();
    if (length >= 0) {
      return length;
    }
    length += 2 * S2.M_PI;
    // Empty intervals have a negative length.
    return (length > 0) ? length : -1;
  }

  /**
   * Return the complement of the interior of the interval. An interval and its
   * complement have the same boundary but do not share any interior values. The
   * complement operator is not a bijection, since the complement of a singleton
   * interval (containing a single value) is the same as the complement of an
   * empty interval.
   */
  public S1Interval complement() {
    if (lo() == hi()) {
      return full(); // Singleton.
    }
    return new S1Interval(hi(), lo(), true); // Handles
    // empty and
    // full.
  }

  /** Return true if the interval (which is closed) contains the point 'p'. */
  public boolean contains(double p) {
    // Works for empty, full, and singleton intervals.
    // assert (Math.abs(p) <= S2.M_PI);
    if (p == -S2.M_PI) {
      p = S2.M_PI;
    }
    return fastContains(p);
  }

  /**
   * Return true if the interval (which is closed) contains the point 'p'. Skips
   * the normalization of 'p' from -Pi to Pi.
   *
   */
  public boolean fastContains(double p) {
    if (isInverted()) {
      return (p >= lo() || p <= hi()) && !isEmpty();
    } else {
      return p >= lo() && p <= hi();
    }
  }

  /** Return true if the interior of the interval contains the point 'p'. */
  public boolean interiorContains(double p) {
    // Works for empty, full, and singleton intervals.
    // assert (Math.abs(p) <= S2.M_PI);
    if (p == -S2.M_PI) {
      p = S2.M_PI;
    }

    if (isInverted()) {
      return p > lo() || p < hi();
    } else {
      return (p > lo() && p < hi()) || isFull();
    }
  }

  /**
   * Return true if the interval contains the given interval 'y'. Works for
   * empty, full, and singleton intervals.
   */
  public boolean contains(final S1Interval y) {
    // It might be helpful to compare the structure of these tests to
    // the simpler Contains(double) method above.

    if (isInverted()) {
      if (y.isInverted()) {
        return y.lo() >= lo() && y.hi() <= hi();
      }
      return (y.lo() >= lo() || y.hi() <= hi()) && !isEmpty();
    } else {
      if (y.isInverted()) {
        return isFull() || y.isEmpty();
      }
      return y.lo() >= lo() && y.hi() <= hi();
    }
  }

  /**
   * Returns true if the interior of this interval contains the entire interval
   * 'y'. Note that x.InteriorContains(x) is true only when x is the empty or
   * full interval, and x.InteriorContains(S1Interval(p,p)) is equivalent to
   * x.InteriorContains(p).
   */
  public boolean interiorContains(final S1Interval y) {
    if (isInverted()) {
      if (!y.isInverted()) {
        return y.lo() > lo() || y.hi() < hi();
      }
      return (y.lo() > lo() && y.hi() < hi()) || y.isEmpty();
    } else {
      if (y.isInverted()) {
        return isFull() || y.isEmpty();
      }
      return (y.lo() > lo() && y.hi() < hi()) || isFull();
    }
  }

  /**
   * Return true if the two intervals contain any points in common. Note that
   * the point +/-Pi has two representations, so the intervals [-Pi,-3] and
   * [2,Pi] intersect, for example.
   */
  public boolean intersects(final S1Interval y) {
    if (isEmpty() || y.isEmpty()) {
      return false;
    }
    if (isInverted()) {
      // Every non-empty inverted interval contains Pi.
      return y.isInverted() || y.lo() <= hi() || y.hi() >= lo();
    } else {
      if (y.isInverted()) {
        return y.lo() <= hi() || y.hi() >= lo();
      }
      return y.lo() <= hi() && y.hi() >= lo();
    }
  }

  /**
   * Return true if the interior of this interval contains any point of the
   * interval 'y' (including its boundary). Works for empty, full, and singleton
   * intervals.
   */
  public boolean interiorIntersects(final S1Interval y) {
    if (isEmpty() || y.isEmpty() || lo() == hi()) {
      return false;
    }
    if (isInverted()) {
      return y.isInverted() || y.lo() < hi() || y.hi() > lo();
    } else {
      if (y.isInverted()) {
        return y.lo() < hi() || y.hi() > lo();
      }
      return (y.lo() < hi() && y.hi() > lo()) || isFull();
    }
  }

  /**
   * Expand the interval by the minimum amount necessary so that it contains the
   * given point "p" (an angle in the range [-Pi, Pi]).
   */
  public S1Interval addPoint(double p) {
    // assert (Math.abs(p) <= S2.M_PI);
    if (p == -S2.M_PI) {
      p = S2.M_PI;
    }

    if (fastContains(p)) {
      return new S1Interval(this);
    }

    if (isEmpty()) {
      return S1Interval.fromPoint(p);
    } else {
      // Compute distance from p to each endpoint.
      double dlo = positiveDistance(p, lo());
      double dhi = positiveDistance(hi(), p);
      if (dlo < dhi) {
        return new S1Interval(p, hi());
      } else {
        return new S1Interval(lo(), p);
      }
      // Adding a point can never turn a non-full interval into a full one.
    }
  }

  /**
   * Return an interval that contains all points within a distance "radius" of
   * a point in this interval. Note that the expansion of an empty interval is
   * always empty. The radius must be non-negative.
   */
  public S1Interval expanded(double radius) {
    // assert (radius >= 0);
    if (isEmpty()) {
      return this;
    }

    // Check whether this interval will be full after expansion, allowing
    // for a 1-bit rounding error when computing each endpoint.
    if (getLength() + 2 * radius >= 2 * S2.M_PI - 1e-15) {
      return full();
    }

    // NOTE(dbeaumont): Should this remainder be 2 * M_PI or just M_PI ??
    double lo = Math.IEEEremainder(lo() - radius, 2 * S2.M_PI);
    double hi = Math.IEEEremainder(hi() + radius, 2 * S2.M_PI);
    if (lo == -S2.M_PI) {
      lo = S2.M_PI;
    }
    return new S1Interval(lo, hi);
  }

  /**
   * Return the smallest interval that contains this interval and the given
   * interval "y".
   */
  public S1Interval union(final S1Interval y) {
    // The y.is_full() case is handled correctly in all cases by the code
    // below, but can follow three separate code paths depending on whether
    // this interval is inverted, is non-inverted but contains Pi, or neither.

    if (y.isEmpty()) {
      return this;
    }
    if (fastContains(y.lo())) {
      if (fastContains(y.hi())) {
        // Either this interval contains y, or the union of the two
        // intervals is the Full() interval.
        if (contains(y)) {
          return this; // is_full() code path
        }
        return full();
      }
      return new S1Interval(lo(), y.hi(), true);
    }
    if (fastContains(y.hi())) {
      return new S1Interval(y.lo(), hi(), true);
    }

    // This interval contains neither endpoint of y. This means that either y
    // contains all of this interval, or the two intervals are disjoint.
    if (isEmpty() || y.fastContains(lo())) {
      return y;
    }

    // Check which pair of endpoints are closer together.
    double dlo = positiveDistance(y.hi(), lo());
    double dhi = positiveDistance(hi(), y.lo());
    if (dlo < dhi) {
      return new S1Interval(y.lo(), hi(), true);
    } else {
      return new S1Interval(lo(), y.hi(), true);
    }
  }

  /**
   * Return the smallest interval that contains the intersection of this
   * interval with "y". Note that the region of intersection may consist of two
   * disjoint intervals.
   */
  public S1Interval intersection(final S1Interval y) {
    // The y.is_full() case is handled correctly in all cases by the code
    // below, but can follow three separate code paths depending on whether
    // this interval is inverted, is non-inverted but contains Pi, or neither.

    if (y.isEmpty()) {
      return empty();
    }
    if (fastContains(y.lo())) {
      if (fastContains(y.hi())) {
        // Either this interval contains y, or the region of intersection
        // consists of two disjoint subintervals. In either case, we want
        // to return the shorter of the two original intervals.
        if (y.getLength() < getLength()) {
          return y; // is_full() code path
        }
        return this;
      }
      return new S1Interval(y.lo(), hi(), true);
    }
    if (fastContains(y.hi())) {
      return new S1Interval(lo(), y.hi(), true);
    }

    // This interval contains neither endpoint of y. This means that either y
    // contains all of this interval, or the two intervals are disjoint.

    if (y.fastContains(lo())) {
      return this; // is_empty() okay here
    }
    // assert (!intersects(y));
    return empty();
  }

  /**
   * Return true if the length of the symmetric difference between the two
   * intervals is at most the given tolerance.
   */
  public boolean approxEquals(final S1Interval y, double maxError) {
    if (isEmpty()) {
      return y.getLength() <= maxError;
    }
    if (y.isEmpty()) {
      return getLength() <= maxError;
    }
    return (Math.abs(Math.IEEEremainder(y.lo() - lo(), 2 * S2.M_PI))
        + Math.abs(Math.IEEEremainder(y.hi() - hi(), 2 * S2.M_PI))) <= maxError;
  }

  public boolean approxEquals(final S1Interval y) {
    return approxEquals(y, 1e-9);
  }

  /**
   * Return true if two intervals contains the same set of points.
   */
  @Override
  public boolean equals(Object that) {
    if (that instanceof S1Interval) {
      S1Interval thatInterval = (S1Interval) that;
      return lo() == thatInterval.lo() && hi() == thatInterval.hi();
    }
    return false;
  }

  @Override
  public int hashCode() {
    long value = 17;
    value = 37 * value + Double.doubleToLongBits(lo());
    value = 37 * value + Double.doubleToLongBits(hi());
    return (int) ((value >>> 32) ^ value);
  }

  @Override
  public String toString() {
    return "[" + this.lo() + ", " + this.hi() + "]";
  }

  /**
   * Compute the distance from "a" to "b" in the range [0, 2*Pi). This is
   * equivalent to (drem(b - a - S2.M_PI, 2 * S2.M_PI) + S2.M_PI), except that
   * it is more numerically stable (it does not lose precision for very small
   * positive distances).
   */
  public static double positiveDistance(double a, double b) {
    double d = b - a;
    if (d >= 0) {
      return d;
    }
    // We want to ensure that if b == Pi and a == (-Pi + eps),
    // the return result is approximately 2*Pi and not zero.
    return (b + S2.M_PI) - (a - S2.M_PI);
  }
}
