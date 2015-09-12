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
 * This class represents a spherical cap, i.e. a portion of a sphere cut off by
 * a plane. The cap is defined by its axis and height. This representation has
 * good numerical accuracy for very small caps (unlike the (axis,
 * min-distance-from-origin) representation), and is also efficient for
 * containment tests (unlike the (axis, angle) representation).
 *
 * Here are some useful relationships between the cap height (h), the cap
 * opening angle (theta), the maximum chord length from the cap's center (d),
 * and the radius of cap's base (a). All formulas assume a unit radius.
 *
 * h = 1 - cos(theta) = 2 sin^2(theta/2) d^2 = 2 h = a^2 + h^2
 *
 */
public final strictfp class S2Cap implements S2Region {

  /**
   * Multiply a positive number by this constant to ensure that the result of a
   * floating point operation is at least as large as the true
   * infinite-precision result.
   */
  private static final double ROUND_UP = 1.0 + 1.0 / (1L << 52);

  private final S2Point axis;
  private final double height;

  // Caps may be constructed from either an axis and a height, or an axis and
  // an angle. To avoid ambiguity, there are no public constructors
  private S2Cap() {
    axis = new S2Point();
    height = 0;
  }

  private S2Cap(S2Point axis, double height) {
    this.axis = axis;
    this.height = height;
    // assert (isValid());
  }

  /**
   * Create a cap given its axis and the cap height, i.e. the maximum projected
   * distance along the cap axis from the cap center. 'axis' should be a
   * unit-length vector.
   */
  public static S2Cap fromAxisHeight(S2Point axis, double height) {
    // assert (S2.isUnitLength(axis));
    return new S2Cap(axis, height);
  }

  /**
   * Create a cap given its axis and the cap opening angle, i.e. maximum angle
   * between the axis and a point on the cap. 'axis' should be a unit-length
   * vector, and 'angle' should be between 0 and 180 degrees.
   */
  public static S2Cap fromAxisAngle(S2Point axis, S1Angle angle) {
    // The height of the cap can be computed as 1-cos(angle), but this isn't
    // very accurate for angles close to zero (where cos(angle) is almost 1).
    // Computing it as 2*(sin(angle/2)**2) gives much better precision.

    // assert (S2.isUnitLength(axis));
    double d = Math.sin(0.5 * angle.radians());
    return new S2Cap(axis, 2 * d * d);

  }

  /**
   * Create a cap given its axis and its area in steradians. 'axis' should be a
   * unit-length vector, and 'area' should be between 0 and 4 * M_PI.
   */
  public static S2Cap fromAxisArea(S2Point axis, double area) {
    // assert (S2.isUnitLength(axis));
    return new S2Cap(axis, area / (2 * S2.M_PI));
  }

  /** Return an empty cap, i.e. a cap that contains no points. */
  public static S2Cap empty() {
    return new S2Cap(new S2Point(1, 0, 0), -1);
  }

  /** Return a full cap, i.e. a cap that contains all points. */
  public static S2Cap full() {
    return new S2Cap(new S2Point(1, 0, 0), 2);
  }


  // Accessor methods.
  public S2Point axis() {
    return axis;
  }

  public double height() {
    return height;
  }

  public double area() {
    return 2 * S2.M_PI * Math.max(0.0, height);
  }

  /**
   * Return the cap opening angle in radians, or a negative number for empty
   * caps.
   */
  public S1Angle angle() {
    // This could also be computed as acos(1 - height_), but the following
    // formula is much more accurate when the cap height is small. It
    // follows from the relationship h = 1 - cos(theta) = 2 sin^2(theta/2).
    if (isEmpty()) {
      return S1Angle.radians(-1);
    }
    return S1Angle.radians(2 * Math.asin(Math.sqrt(0.5 * height)));
  }

  /**
   * We allow negative heights (to represent empty caps) but not heights greater
   * than 2.
   */
  public boolean isValid() {
    return S2.isUnitLength(axis) && height <= 2;
  }

  /** Return true if the cap is empty, i.e. it contains no points. */
  public boolean isEmpty() {
    return height < 0;
  }

  /** Return true if the cap is full, i.e. it contains all points. */
  public boolean isFull() {
    return height >= 2;
  }

  /**
   * Return the complement of the interior of the cap. A cap and its complement
   * have the same boundary but do not share any interior points. The complement
   * operator is not a bijection, since the complement of a singleton cap
   * (containing a single point) is the same as the complement of an empty cap.
   */
  public S2Cap complement() {
    // The complement of a full cap is an empty cap, not a singleton.
    // Also make sure that the complement of an empty cap has height 2.
    double cHeight = isFull() ? -1 : 2 - Math.max(height, 0.0);
    return S2Cap.fromAxisHeight(S2Point.neg(axis), cHeight);
  }

  /**
   * Return true if and only if this cap contains the given other cap (in a set
   * containment sense, e.g. every cap contains the empty cap).
   */
  public boolean contains(S2Cap other) {
    if (isFull() || other.isEmpty()) {
      return true;
    }
    return angle().radians() >= axis.angle(other.axis)
      + other.angle().radians();
  }

  /**
   * Return true if and only if the interior of this cap intersects the given
   * other cap. (This relationship is not symmetric, since only the interior of
   * this cap is used.)
   */
  public boolean interiorIntersects(S2Cap other) {
    // Interior(X) intersects Y if and only if Complement(Interior(X))
    // does not contain Y.
    return !complement().contains(other);
  }

  /**
   * Return true if and only if the given point is contained in the interior of
   * the region (i.e. the region excluding its boundary). 'p' should be a
   * unit-length vector.
   */
  public boolean interiorContains(S2Point p) {
    // assert (S2.isUnitLength(p));
    return isFull() || S2Point.sub(axis, p).norm2() < 2 * height;
  }

  /**
   * Increase the cap height if necessary to include the given point. If the cap
   * is empty the axis is set to the given point, but otherwise it is left
   * unchanged. 'p' should be a unit-length vector.
   */
  public S2Cap addPoint(S2Point p) {
    // Compute the squared chord length, then convert it into a height.
    // assert (S2.isUnitLength(p));
    if (isEmpty()) {
      return new S2Cap(p, 0);
    } else {
      // To make sure that the resulting cap actually includes this point,
      // we need to round up the distance calculation. That is, after
      // calling cap.AddPoint(p), cap.Contains(p) should be true.
      double dist2 = S2Point.sub(axis, p).norm2();
      double newHeight = Math.max(height, ROUND_UP * 0.5 * dist2);
      return new S2Cap(axis, newHeight);
    }
  }

  // Increase the cap height if necessary to include "other". If the current
  // cap is empty it is set to the given other cap.
  public S2Cap addCap(S2Cap other) {
    if (isEmpty()) {
      return new S2Cap(other.axis, other.height);
    } else {
      // See comments for FromAxisAngle() and AddPoint(). This could be
      // optimized by doing the calculation in terms of cap heights rather
      // than cap opening angles.
      double angle = axis.angle(other.axis) + other.angle().radians();
      if (angle >= S2.M_PI) {
        return new S2Cap(axis, 2); //Full cap
      } else {
        double d = Math.sin(0.5 * angle);
        double newHeight = Math.max(height, ROUND_UP * 2 * d * d);
        return new S2Cap(axis, newHeight);
      }
    }
  }

  // //////////////////////////////////////////////////////////////////////
  // S2Region interface (see {@code S2Region} for details):
  @Override
  public S2Cap getCapBound() {
    return this;
  }

  @Override
  public S2LatLngRect getRectBound() {
    if (isEmpty()) {
      return S2LatLngRect.empty();
    }

    // Convert the axis to a (lat,lng) pair, and compute the cap angle.
    S2LatLng axisLatLng = new S2LatLng(axis);
    double capAngle = angle().radians();

    boolean allLongitudes = false;
    double[] lat = new double[2], lng = new double[2];
    lng[0] = -S2.M_PI;
    lng[1] = S2.M_PI;

    // Check whether cap includes the south pole.
    lat[0] = axisLatLng.lat().radians() - capAngle;
    if (lat[0] <= -S2.M_PI_2) {
      lat[0] = -S2.M_PI_2;
      allLongitudes = true;
    }
    // Check whether cap includes the north pole.
    lat[1] = axisLatLng.lat().radians() + capAngle;
    if (lat[1] >= S2.M_PI_2) {
      lat[1] = S2.M_PI_2;
      allLongitudes = true;
    }
    if (!allLongitudes) {
      // Compute the range of longitudes covered by the cap. We use the law
      // of sines for spherical triangles. Consider the triangle ABC where
      // A is the north pole, B is the center of the cap, and C is the point
      // of tangency between the cap boundary and a line of longitude. Then
      // C is a right angle, and letting a,b,c denote the sides opposite A,B,C,
      // we have sin(a)/sin(A) = sin(c)/sin(C), or sin(A) = sin(a)/sin(c).
      // Here "a" is the cap angle, and "c" is the colatitude (90 degrees
      // minus the latitude). This formula also works for negative latitudes.
      //
      // The formula for sin(a) follows from the relationship h = 1 - cos(a).

      double sinA = Math.sqrt(height * (2 - height));
      double sinC = Math.cos(axisLatLng.lat().radians());
      if (sinA <= sinC) {
        double angleA = Math.asin(sinA / sinC);
        lng[0] = Math.IEEEremainder(axisLatLng.lng().radians() - angleA,
          2 * S2.M_PI);
        lng[1] = Math.IEEEremainder(axisLatLng.lng().radians() + angleA,
          2 * S2.M_PI);
      }
    }
    return new S2LatLngRect(new R1Interval(lat[0], lat[1]), new S1Interval(
      lng[0], lng[1]));
  }

  @Override
  public boolean contains(S2Cell cell) {
    // If the cap does not contain all cell vertices, return false.
    // We check the vertices before taking the Complement() because we can't
    // accurately represent the complement of a very small cap (a height
    // of 2-epsilon is rounded off to 2).
    S2Point[] vertices = new S2Point[4];
    for (int k = 0; k < 4; ++k) {
      vertices[k] = cell.getVertex(k);
      if (!contains(vertices[k])) {
        return false;
      }
    }
    // Otherwise, return true if the complement of the cap does not intersect
    // the cell. (This test is slightly conservative, because technically we
    // want Complement().InteriorIntersects() here.)
    return !complement().intersects(cell, vertices);
  }

  @Override
  public boolean mayIntersect(S2Cell cell) {
    // If the cap contains any cell vertex, return true.
    S2Point[] vertices = new S2Point[4];
    for (int k = 0; k < 4; ++k) {
      vertices[k] = cell.getVertex(k);
      if (contains(vertices[k])) {
        return true;
      }
    }
    return intersects(cell, vertices);
  }

  /**
   * Return true if the cap intersects 'cell', given that the cap vertices have
   * alrady been checked.
   */
  public boolean intersects(S2Cell cell, S2Point[] vertices) {
    // Return true if this cap intersects any point of 'cell' excluding its
    // vertices (which are assumed to already have been checked).

    // If the cap is a hemisphere or larger, the cell and the complement of the
    // cap are both convex. Therefore since no vertex of the cell is contained,
    // no other interior point of the cell is contained either.
    if (height >= 1) {
      return false;
    }

    // We need to check for empty caps due to the axis check just below.
    if (isEmpty()) {
      return false;
    }

    // Optimization: return true if the cell contains the cap axis. (This
    // allows half of the edge checks below to be skipped.)
    if (cell.contains(axis)) {
      return true;
    }

    // At this point we know that the cell does not contain the cap axis,
    // and the cap does not contain any cell vertex. The only way that they
    // can intersect is if the cap intersects the interior of some edge.

    double sin2Angle = height * (2 - height); // sin^2(capAngle)
    for (int k = 0; k < 4; ++k) {
      S2Point edge = cell.getEdgeRaw(k);
      double dot = axis.dotProd(edge);
      if (dot > 0) {
        // The axis is in the interior half-space defined by the edge. We don't
        // need to consider these edges, since if the cap intersects this edge
        // then it also intersects the edge on the opposite side of the cell
        // (because we know the axis is not contained with the cell).
        continue;
      }
      // The Norm2() factor is necessary because "edge" is not normalized.
      if (dot * dot > sin2Angle * edge.norm2()) {
        return false; // Entire cap is on the exterior side of this edge.
      }
      // Otherwise, the great circle containing this edge intersects
      // the interior of the cap. We just need to check whether the point
      // of closest approach occurs between the two edge endpoints.
      S2Point dir = S2Point.crossProd(edge, axis);
      if (dir.dotProd(vertices[k]) < 0
          && dir.dotProd(vertices[(k + 1) & 3]) > 0) {
        return true;
      }
    }
    return false;
  }

  public boolean contains(S2Point p) {
    // The point 'p' should be a unit-length vector.
    // assert (S2.isUnitLength(p));
    return S2Point.sub(axis, p).norm2() <= 2 * height;

  }


  /** Return true if two caps are identical. */
  @Override
  public boolean equals(Object that) {

    if (!(that instanceof S2Cap)) {
      return false;
    }

    S2Cap other = (S2Cap) that;
    return (axis.equals(other.axis) && height == other.height)
        || (isEmpty() && other.isEmpty()) || (isFull() && other.isFull());

  }

  @Override
  public int hashCode() {
    if (isFull()) {
      return 17;
    } else if (isEmpty()) {
      return 37;
    }
    int result = 17;
    result = 37 * result + axis.hashCode();
    long heightBits = Double.doubleToLongBits(height);
    result = 37 * result + (int) ((heightBits >>> 32) ^ heightBits);
    return result;
  }

  // /////////////////////////////////////////////////////////////////////
  // The following static methods are convenience functions for assertions
  // and testing purposes only.

  /**
   * Return true if the cap axis and height differ by at most "max_error" from
   * the given cap "other".
   */
  boolean approxEquals(S2Cap other, double maxError) {
    return (axis.aequal(other.axis, maxError) && Math.abs(height - other.height) <= maxError)
      || (isEmpty() && other.height <= maxError)
      || (other.isEmpty() && height <= maxError)
      || (isFull() && other.height >= 2 - maxError)
      || (other.isFull() && height >= 2 - maxError);
  }

  boolean approxEquals(S2Cap other) {
    return approxEquals(other, 1e-14);
  }

  @Override
  public String toString() {
    return "[Point = " + axis.toString() + " Height = " + height + "]";
  }
}
