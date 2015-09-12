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
 * R2Vector represents a vector in the two-dimensional space. It defines the
 * basic geometrical operations for 2D vectors, e.g. cross product, addition,
 * norm, comparison etc.
 *
 */
public final strictfp class R2Vector {
  private final double x;
  private final double y;

  public R2Vector() {
    this(0, 0);
  }

  public R2Vector(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public R2Vector(double[] coord) {
    if (coord.length != 2) {
      throw new IllegalStateException("Points must have exactly 2 coordinates");
    }
    x = coord[0];
    y = coord[1];
  }

  public double x() {
    return x;
  }

  public double y() {
    return y;
  }

  public double get(int index) {
    if (index > 1) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return index == 0 ? this.x : this.y;
  }

  public static R2Vector add(final R2Vector p1, final R2Vector p2) {
    return new R2Vector(p1.x + p2.x, p1.y + p2.y);
  }

  public static R2Vector mul(final R2Vector p, double m) {
    return new R2Vector(m * p.x, m * p.y);
  }

  public double norm2() {
    return (x * x) + (y * y);
  }

  public static double dotProd(final R2Vector p1, final R2Vector p2) {
    return (p1.x * p2.x) + (p1.y * p2.y);
  }

  public double dotProd(R2Vector that) {
    return dotProd(this, that);
  }

  public double crossProd(final R2Vector that) {
    return this.x * that.y - this.y * that.x;
  }

  public boolean lessThan(R2Vector vb) {
    if (x < vb.x) {
      return true;
    }
    if (vb.x < x) {
      return false;
    }
    if (y < vb.y) {
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof R2Vector)) {
      return false;
    }
    R2Vector thatPoint = (R2Vector) that;
    return this.x == thatPoint.x && this.y == thatPoint.y;
  }

  /**
   * Calcualates hashcode based on stored coordinates. Since we want +0.0 and
   * -0.0 to be treated the same, we ignore the sign of the coordinates.
   */
  @Override
  public int hashCode() {
    long value = 17;
    value += 37 * value + Double.doubleToLongBits(Math.abs(x));
    value += 37 * value + Double.doubleToLongBits(Math.abs(y));
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public String toString() {
    return "(" + x + ", " + y + ")";
  }
}
