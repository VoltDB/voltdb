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

import com.google_voltpatches.common.base.Preconditions;

/**
 * An S2LatLngRect represents a latitude-longitude rectangle. It is capable of
 * representing the empty and full rectangles as well as single points.
 *
 */

public strictfp class S2LatLngRect implements S2Region {

  private final R1Interval lat;
  private final S1Interval lng;

  /**
   * Construct a rectangle from minimum and maximum latitudes and longitudes. If
   * lo.lng() > hi.lng(), the rectangle spans the 180 degree longitude line.
   */
  public S2LatLngRect(final S2LatLng lo, final S2LatLng hi) {
    lat = new R1Interval(lo.lat().radians(), hi.lat().radians());
    lng = new S1Interval(lo.lng().radians(), hi.lng().radians());
    // assert (isValid());
  }

  /** Construct a rectangle from latitude and longitude intervals. */
  public S2LatLngRect(R1Interval lat, S1Interval lng) {
    this.lat = lat;
    this.lng = lng;
    // assert (isValid());
  }

  /** The canonical empty rectangle */
  public static S2LatLngRect empty() {
    return new S2LatLngRect(R1Interval.empty(), S1Interval.empty());
  }

  /** The canonical full rectangle. */
  public static S2LatLngRect full() {
    return new S2LatLngRect(fullLat(), fullLng());
  }

  /** The full allowable range of latitudes. */
  public static R1Interval fullLat() {
    return new R1Interval(-S2.M_PI_2, S2.M_PI_2);
  }

  /**
   * The full allowable range of longitudes.
   */
  public static S1Interval fullLng() {
    return S1Interval.full();
  }

  /**
   * Construct a rectangle from a center point (in lat-lng space) and size in
   * each dimension. If size.lng() is greater than 360 degrees it is clamped,
   * and latitudes greater than +/- 90 degrees are also clamped. So for example,
   * FromCenterSize((80,170),(20,20)) -> (lo=(60,150),hi=(90,-170)).
   */
  public static S2LatLngRect fromCenterSize(S2LatLng center, S2LatLng size) {
    return fromPoint(center).expanded(size.mul(0.5));
  }

  /** Convenience method to construct a rectangle containing a single point. */
  public static S2LatLngRect fromPoint(S2LatLng p) {
    // assert (p.isValid());
    return new S2LatLngRect(p, p);
  }

  /**
   * Convenience method to construct the minimal bounding rectangle containing
   * the two given points. This is equivalent to starting with an empty
   * rectangle and calling AddPoint() twice. Note that it is different than the
   * S2LatLngRect(lo, hi) constructor, where the first point is always used as
   * the lower-left corner of the resulting rectangle.
   */
  public static S2LatLngRect fromPointPair(S2LatLng p1, S2LatLng p2) {
    // assert (p1.isValid() && p2.isValid());
    return new S2LatLngRect(R1Interval.fromPointPair(p1.lat().radians(), p2
      .lat().radians()), S1Interval.fromPointPair(p1.lng().radians(), p2.lng()
      .radians()));
  }

  /**
   * Return a latitude-longitude rectangle that contains the edge from "a" to
   * "b". Both points must be unit-length. Note that the bounding rectangle of
   * an edge can be larger than the bounding rectangle of its endpoints.
   */
  public static S2LatLngRect fromEdge(S2Point a, S2Point b) {
    // assert (S2.isUnitLength(a) && S2.isUnitLength(b));
    S2LatLngRect r = fromPointPair(new S2LatLng(a), new S2LatLng(b));

    // Check whether the min/max latitude occurs in the edge interior.
    // We find the normal to the plane containing AB, and then a vector "dir" in
    // this plane that also passes through the equator. We use RobustCrossProd
    // to ensure that the edge normal is accurate even when the two points are
    // very close together.
    S2Point ab = S2.robustCrossProd(a, b);
    S2Point dir = S2Point.crossProd(ab, new S2Point(0, 0, 1));
    double da = dir.dotProd(a);
    double db = dir.dotProd(b);
    if (da * db >= 0) {
      // Minimum and maximum latitude are attained at the vertices.
      return r;
    }
    // Minimum/maximum latitude occurs in the edge interior. This affects the
    // latitude bounds but not the longitude bounds.
    double absLat = Math.acos(Math.abs(ab.z / ab.norm()));
    if (da < 0) {
      return new S2LatLngRect(new R1Interval(r.lat().lo(), absLat), r.lng());
    } else {
      return new S2LatLngRect(new R1Interval(-absLat, r.lat().hi()), r.lng());
    }
  }

  /**
   * Return true if the rectangle is valid, which essentially just means that
   * the latitude bounds do not exceed Pi/2 in absolute value and the longitude
   * bounds do not exceed Pi in absolute value.
   *
   */
  public boolean isValid() {
    // The lat/lng ranges must either be both empty or both non-empty.
    return (Math.abs(lat.lo()) <= S2.M_PI_2 && Math.abs(lat.hi()) <= S2.M_PI_2
      && lng.isValid() && lat.isEmpty() == lng.isEmpty());
  }

  // Accessor methods.
  public S1Angle latLo() {
    return S1Angle.radians(lat.lo());
  }

  public S1Angle latHi() {
    return S1Angle.radians(lat.hi());
  }

  public S1Angle lngLo() {
    return S1Angle.radians(lng.lo());
  }

  public S1Angle lngHi() {
    return S1Angle.radians(lng.hi());
  }

  public R1Interval lat() {
    return lat;
  }

  public S1Interval lng() {
    return lng;
  }

  public S2LatLng lo() {
    return new S2LatLng(latLo(), lngLo());
  }

  public S2LatLng hi() {
    return new S2LatLng(latHi(), lngHi());
  }

  /**
   * Return true if the rectangle is empty, i.e. it contains no points at all.
   */
  public boolean isEmpty() {
    return lat.isEmpty();
  }

  // Return true if the rectangle is full, i.e. it contains all points.
  public boolean isFull() {
    return lat.equals(fullLat()) && lng.isFull();
  }

  /**
   * Return true if lng_.lo() > lng_.hi(), i.e. the rectangle crosses the 180
   * degree latitude line.
   */
  public boolean isInverted() {
    return lng.isInverted();
  }

  /** Return the k-th vertex of the rectangle (k = 0,1,2,3) in CCW order. */
  public S2LatLng getVertex(int k) {
    // Return the points in CCW order (SW, SE, NE, NW).
    switch (k) {
      case 0:
        return S2LatLng.fromRadians(lat.lo(), lng.lo());
      case 1:
        return S2LatLng.fromRadians(lat.lo(), lng.hi());
      case 2:
        return S2LatLng.fromRadians(lat.hi(), lng.hi());
      case 3:
        return S2LatLng.fromRadians(lat.hi(), lng.lo());
      default:
        throw new IllegalArgumentException("Invalid vertex index.");
    }
  }

  /**
   * Return the center of the rectangle in latitude-longitude space (in general
   * this is not the center of the region on the sphere).
   */
  public S2LatLng getCenter() {
    return S2LatLng.fromRadians(lat.getCenter(), lng.getCenter());
  }

  /**
   * Return the minimum distance (measured along the surface of the sphere)
   * from a given point to the rectangle (both its boundary and its interior).
   * The latLng must be valid.
   */
  public S1Angle getDistance(S2LatLng p) {
    // The algorithm here is the same as in getDistance(S2LagLngRect), only
    // with simplified calculations.
    S2LatLngRect a = this;

    Preconditions.checkState(!a.isEmpty());
    Preconditions.checkArgument(p.isValid());

    if (a.lng().contains(p.lng().radians())) {
      return S1Angle.radians(Math.max(0.0, Math.max(p.lat().radians() - a.lat().hi(),
                                                    a.lat().lo() - p.lat().radians())));
    }

    S1Interval interval = new S1Interval(a.lng().hi(), a.lng().complement().getCenter());
    double aLng = a.lng().lo();
    if (interval.contains(p.lng().radians())) {
      aLng = a.lng().hi();
    }

    S2Point lo = S2LatLng.fromRadians(a.lat().lo(), aLng).toPoint();
    S2Point hi = S2LatLng.fromRadians(a.lat().hi(), aLng).toPoint();
    S2Point loCrossHi =
        S2LatLng.fromRadians(0, aLng - S2.M_PI_2).normalized().toPoint();
    return S2EdgeUtil.getDistance(p.toPoint(), lo, hi, loCrossHi);
  }

  /**
   * Return the minimum distance (measured along the surface of the sphere) to
   * the given S2LatLngRect. Both S2LatLngRects must be non-empty.
   */
  public S1Angle getDistance(S2LatLngRect other) {
    S2LatLngRect a = this;
    S2LatLngRect b = other;

    Preconditions.checkState(!a.isEmpty());
    Preconditions.checkArgument(!b.isEmpty());

    // First, handle the trivial cases where the longitude intervals overlap.
    if (a.lng().intersects(b.lng())) {
      if (a.lat().intersects(b.lat())) {
        return S1Angle.radians(0);  // Intersection between a and b.
      }

      // We found an overlap in the longitude interval, but not in the latitude
      // interval. This means the shortest path travels along some line of
      // longitude connecting the high-latitude of the lower rect with the
      // low-latitude of the higher rect.
      S1Angle lo, hi;
      if (a.lat().lo() > b.lat().hi()) {
        lo = b.latHi();
        hi = a.latLo();
      } else {
        lo = a.latHi();
        hi = b.latLo();
      }
      return S1Angle.radians(hi.radians() - lo.radians());
    }

    // The longitude intervals don't overlap. In this case, the closest points
    // occur somewhere on the pair of longitudinal edges which are nearest in
    // longitude-space.
    S1Angle aLng, bLng;
    S1Interval loHi = S1Interval.fromPointPair(a.lng().lo(), b.lng().hi());
    S1Interval hiLo = S1Interval.fromPointPair(a.lng().hi(), b.lng().lo());
    if (loHi.getLength() < hiLo.getLength()) {
      aLng = a.lngLo();
      bLng = b.lngHi();
    } else {
      aLng = a.lngHi();
      bLng = b.lngLo();
    }

    // The shortest distance between the two longitudinal segments will include
    // at least one segment endpoint. We could probably narrow this down further
    // to a single point-edge distance by comparing the relative latitudes of the
    // endpoints, but for the sake of clarity, we'll do all four point-edge
    // distance tests.
    S2Point aLo = new S2LatLng(a.latLo(), aLng).toPoint();
    S2Point aHi = new S2LatLng(a.latHi(), aLng).toPoint();
    S2Point aLoCrossHi =
        S2LatLng.fromRadians(0, aLng.radians() - S2.M_PI_2).normalized().toPoint();
    S2Point bLo = new S2LatLng(b.latLo(), bLng).toPoint();
    S2Point bHi = new S2LatLng(b.latHi(), bLng).toPoint();
    S2Point bLoCrossHi =
        S2LatLng.fromRadians(0, bLng.radians() - S2.M_PI_2).normalized().toPoint();

    return S1Angle.min(S2EdgeUtil.getDistance(aLo, bLo, bHi, bLoCrossHi),
                       S1Angle.min(S2EdgeUtil.getDistance(aHi, bLo, bHi, bLoCrossHi),
                                   S1Angle.min(S2EdgeUtil.getDistance(bLo, aLo, aHi, aLoCrossHi),
                                               S2EdgeUtil.getDistance(bHi, aLo, aHi, aLoCrossHi))));
  }

  /**
   * Return the width and height of this rectangle in latitude-longitude space.
   * Empty rectangles have a negative width and height.
   */
  public S2LatLng getSize() {
    return S2LatLng.fromRadians(lat.getLength(), lng.getLength());
  }

  /**
   * More efficient version of Contains() that accepts a S2LatLng rather than an
   * S2Point.
   */
  public boolean contains(S2LatLng ll) {
    // assert (ll.isValid());
    return (lat.contains(ll.lat().radians()) && lng.contains(ll.lng()
      .radians()));

  }

  /**
   * Return true if and only if the given point is contained in the interior of
   * the region (i.e. the region excluding its boundary). The point 'p' does not
   * need to be normalized.
   */
  public boolean interiorContains(S2Point p) {
    return interiorContains(new S2LatLng(p));
  }

  /**
   * More efficient version of InteriorContains() that accepts a S2LatLng rather
   * than an S2Point.
   */
  public boolean interiorContains(S2LatLng ll) {
    // assert (ll.isValid());
    return (lat.interiorContains(ll.lat().radians()) && lng
      .interiorContains(ll.lng().radians()));
  }

  /**
   * Return true if and only if the rectangle contains the given other
   * rectangle.
   */
  public boolean contains(S2LatLngRect other) {
    return lat.contains(other.lat) && lng.contains(other.lng);
  }

  /**
   * Return true if and only if the interior of this rectangle contains all
   * points of the given other rectangle (including its boundary).
   */
  public boolean interiorContains(S2LatLngRect other) {
    return (lat.interiorContains(other.lat) && lng
      .interiorContains(other.lng));
  }

  /** Return true if this rectangle and the given other rectangle have any
  points in common. */
  public boolean intersects(S2LatLngRect other) {
    return lat.intersects(other.lat) && lng.intersects(other.lng);
  }

  /**
   * Returns true if this rectangle intersects the given cell. (This is an exact
   * test and may be fairly expensive, see also MayIntersect below.)
   */
  public boolean intersects(S2Cell cell) {
    // First we eliminate the cases where one region completely contains the
    // other. Once these are disposed of, then the regions will intersect
    // if and only if their boundaries intersect.

    if (isEmpty()) {
      return false;
    }
    if (contains(cell.getCenter())) {
      return true;
    }
    if (cell.contains(getCenter().toPoint())) {
      return true;
    }

    // Quick rejection test (not required for correctness).
    if (!intersects(cell.getRectBound())) {
      return false;
    }

    // Now check whether the boundaries intersect. Unfortunately, a
    // latitude-longitude rectangle does not have straight edges -- two edges
    // are curved, and at least one of them is concave.

    // Precompute the cell vertices as points and latitude-longitudes.
    S2Point[] cellV = new S2Point[4];
    S2LatLng[] cellLl = new S2LatLng[4];
    for (int i = 0; i < 4; ++i) {
      cellV[i] = cell.getVertex(i); // Must be normalized.
      cellLl[i] = new S2LatLng(cellV[i]);
      if (contains(cellLl[i])) {
        return true; // Quick acceptance test.
      }
    }

    for (int i = 0; i < 4; ++i) {
      S1Interval edgeLng = S1Interval.fromPointPair(
        cellLl[i].lng().radians(), cellLl[(i + 1) & 3].lng().radians());
      if (!lng.intersects(edgeLng)) {
        continue;
      }

      final S2Point a = cellV[i];
      final S2Point b = cellV[(i + 1) & 3];
      if (edgeLng.contains(lng.lo())) {
        if (intersectsLngEdge(a, b, lat, lng.lo())) {
          return true;
        }
      }
      if (edgeLng.contains(lng.hi())) {
        if (intersectsLngEdge(a, b, lat, lng.hi())) {
          return true;
        }
      }
      if (intersectsLatEdge(a, b, lat.lo(), lng)) {
        return true;
      }
      if (intersectsLatEdge(a, b, lat.hi(), lng)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return true if and only if the interior of this rectangle intersects any
   * point (including the boundary) of the given other rectangle.
   */
  public boolean interiorIntersects(S2LatLngRect other) {
    return (lat.interiorIntersects(other.lat) && lng
      .interiorIntersects(other.lng));
  }

  public S2LatLngRect addPoint(S2Point p) {
    return addPoint(new S2LatLng(p));
  }

  // Increase the size of the bounding rectangle to include the given point.
  // The rectangle is expanded by the minimum amount possible.
  public S2LatLngRect addPoint(S2LatLng ll) {
    // assert (ll.isValid());
    R1Interval newLat = lat.addPoint(ll.lat().radians());
    S1Interval newLng = lng.addPoint(ll.lng().radians());
    return new S2LatLngRect(newLat, newLng);
  }

  /**
   * Return a rectangle that contains all points whose latitude distance from
   * this rectangle is at most margin.lat(), and whose longitude distance from
   * this rectangle is at most margin.lng(). In particular, latitudes are
   * clamped while longitudes are wrapped. Note that any expansion of an empty
   * interval remains empty, and both components of the given margin must be
   * non-negative.
   *
   * NOTE: If you are trying to grow a rectangle by a certain *distance* on the
   * sphere (e.g. 5km), use the ConvolveWithCap() method instead.
   */
  public S2LatLngRect expanded(S2LatLng margin) {
    // assert (margin.lat().radians() >= 0 && margin.lng().radians() >= 0);
    if (isEmpty()) {
      return this;
    }
    return new S2LatLngRect(lat.expanded(margin.lat().radians()).intersection(
      fullLat()), lng.expanded(margin.lng().radians()));
  }

  /**
   * Return the smallest rectangle containing the union of this rectangle and
   * the given rectangle.
   */
  public S2LatLngRect union(S2LatLngRect other) {
    return new S2LatLngRect(lat.union(other.lat), lng.union(other.lng));
  }

  /**
   * Return the smallest rectangle containing the intersection of this rectangle
   * and the given rectangle. Note that the region of intersection may consist
   * of two disjoint rectangles, in which case a single rectangle spanning both
   * of them is returned.
   */
  public S2LatLngRect intersection(S2LatLngRect other) {
    R1Interval intersectLat = lat.intersection(other.lat);
    S1Interval intersectLng = lng.intersection(other.lng);
    if (intersectLat.isEmpty() || intersectLng.isEmpty()) {
      // The lat/lng ranges must either be both empty or both non-empty.
      return empty();
    }
    return new S2LatLngRect(intersectLat, intersectLng);
  }

  /**
   * Return a rectangle that contains the convolution of this rectangle with a
   * cap of the given angle. This expands the rectangle by a fixed distance (as
   * opposed to growing the rectangle in latitude-longitude space). The returned
   * rectangle includes all points whose minimum distance to the original
   * rectangle is at most the given angle.
   */
  public S2LatLngRect convolveWithCap(S1Angle angle) {
    // The most straightforward approach is to build a cap centered on each
    // vertex and take the union of all the bounding rectangles (including the
    // original rectangle; this is necessary for very large rectangles).

    // Optimization: convert the angle to a height exactly once.
    S2Cap cap = S2Cap.fromAxisAngle(new S2Point(1, 0, 0), angle);

    S2LatLngRect r = this;
    for (int k = 0; k < 4; ++k) {
      S2Cap vertexCap = S2Cap.fromAxisHeight(getVertex(k).toPoint(), cap
        .height());
      r = r.union(vertexCap.getRectBound());
    }
    return r;
  }

  /** Return the surface area of this rectangle on the unit sphere. */
  public double area() {
    if (isEmpty()) {
      return 0;
    }

    // This is the size difference of the two spherical caps, multiplied by
    // the longitude ratio.
    return lng().getLength() * Math.abs(Math.sin(latHi().radians()) - Math.sin(latLo().radians()));
  }

  /** Return true if two rectangles contains the same set of points. */
  @Override
  public boolean equals(Object that) {
    if (!(that instanceof S2LatLngRect)) {
      return false;
    }
    S2LatLngRect otherRect = (S2LatLngRect) that;
    return lat().equals(otherRect.lat()) && lng().equals(otherRect.lng());
  }

  /**
   * Return true if the latitude and longitude intervals of the two rectangles
   * are the same up to the given tolerance (see r1interval.h and s1interval.h
   * for details).
   */
  public boolean approxEquals(S2LatLngRect other, double maxError) {
    return (lat.approxEquals(other.lat, maxError) && lng.approxEquals(
      other.lng, maxError));
  }

  public boolean approxEquals(S2LatLngRect other) {
    return approxEquals(other, 1e-15);
  }

  @Override
  public int hashCode() {
    int value = 17;
    value = 37 * value + lat.hashCode();
    return (37 * value + lng.hashCode());
  }

  // //////////////////////////////////////////////////////////////////////
  // S2Region interface (see {@code S2Region} for details):

  @Override
  public S2Region clone() {
    return new S2LatLngRect(this.lo(), this.hi());
  }

  @Override
  public S2Cap getCapBound() {
    // We consider two possible bounding caps, one whose axis passes
    // through the center of the lat-long rectangle and one whose axis
    // is the north or south pole. We return the smaller of the two caps.

    if (isEmpty()) {
      return S2Cap.empty();
    }

    double poleZ, poleAngle;
    if (lat.lo() + lat.hi() < 0) {
      // South pole axis yields smaller cap.
      poleZ = -1;
      poleAngle = S2.M_PI_2 + lat.hi();
    } else {
      poleZ = 1;
      poleAngle = S2.M_PI_2 - lat.lo();
    }
    S2Cap poleCap = S2Cap.fromAxisAngle(new S2Point(0, 0, poleZ), S1Angle
      .radians(poleAngle));

    // For bounding rectangles that span 180 degrees or less in longitude, the
    // maximum cap size is achieved at one of the rectangle vertices. For
    // rectangles that are larger than 180 degrees, we punt and always return a
    // bounding cap centered at one of the two poles.
    double lngSpan = lng.hi() - lng.lo();
    if (Math.IEEEremainder(lngSpan, 2 * S2.M_PI) >= 0) {
      if (lngSpan < 2 * S2.M_PI) {
        S2Cap midCap = S2Cap.fromAxisAngle(getCenter().toPoint(), S1Angle
          .radians(0));
        for (int k = 0; k < 4; ++k) {
          midCap = midCap.addPoint(getVertex(k).toPoint());
        }
        if (midCap.height() < poleCap.height()) {
          return midCap;
        }
      }
    }
    return poleCap;
  }

  @Override
  public S2LatLngRect getRectBound() {
    return this;
  }

  @Override
  public boolean contains(S2Cell cell) {
    // A latitude-longitude rectangle contains a cell if and only if it contains
    // the cell's bounding rectangle. (This is an exact test.)
    return contains(cell.getRectBound());
  }

  /**
   * This test is cheap but is NOT exact. Use Intersects() if you want a more
   * accurate and more expensive test. Note that when this method is used by an
   * S2RegionCoverer, the accuracy isn't all that important since if a cell may
   * intersect the region then it is subdivided, and the accuracy of this method
   * goes up as the cells get smaller.
   */
  @Override
  public boolean mayIntersect(S2Cell cell) {
    // This test is cheap but is NOT exact (see s2latlngrect.h).
    return intersects(cell.getRectBound());
  }

  /** The point 'p' does not need to be normalized. */
  public boolean contains(S2Point p) {
    return contains(new S2LatLng(p));
  }

  /**
   * Return true if the edge AB intersects the given edge of constant longitude.
   */
  private static boolean intersectsLngEdge(S2Point a, S2Point b,
      R1Interval lat, double lng) {
    // Return true if the segment AB intersects the given edge of constant
    // longitude. The nice thing about edges of constant longitude is that
    // they are straight lines on the sphere (geodesics).

    return S2.simpleCrossing(a, b, S2LatLng.fromRadians(lat.lo(), lng)
      .toPoint(), S2LatLng.fromRadians(lat.hi(), lng).toPoint());
  }

  /**
   * Return true if the edge AB intersects the given edge of constant latitude.
   */
  private static boolean intersectsLatEdge(S2Point a, S2Point b, double lat,
      S1Interval lng) {
    // Return true if the segment AB intersects the given edge of constant
    // latitude. Unfortunately, lines of constant latitude are curves on
    // the sphere. They can intersect a straight edge in 0, 1, or 2 points.
    // assert (S2.isUnitLength(a) && S2.isUnitLength(b));

    // First, compute the normal to the plane AB that points vaguely north.
    S2Point z = S2Point.normalize(S2.robustCrossProd(a, b));
    if (z.z < 0) {
      z = S2Point.neg(z);
    }

    // Extend this to an orthonormal frame (x,y,z) where x is the direction
    // where the great circle through AB achieves its maximium latitude.
    S2Point y = S2Point.normalize(S2.robustCrossProd(z, new S2Point(0, 0, 1)));
    S2Point x = S2Point.crossProd(y, z);
    // assert (S2.isUnitLength(x) && x.z >= 0);

    // Compute the angle "theta" from the x-axis (in the x-y plane defined
    // above) where the great circle intersects the given line of latitude.
    double sinLat = Math.sin(lat);
    if (Math.abs(sinLat) >= x.z) {
      return false; // The great circle does not reach the given latitude.
    }
    // assert (x.z > 0);
    double cosTheta = sinLat / x.z;
    double sinTheta = Math.sqrt(1 - cosTheta * cosTheta);
    double theta = Math.atan2(sinTheta, cosTheta);

    // The candidate intersection points are located +/- theta in the x-y
    // plane. For an intersection to be valid, we need to check that the
    // intersection point is contained in the interior of the edge AB and
    // also that it is contained within the given longitude interval "lng".

    // Compute the range of theta values spanned by the edge AB.
    S1Interval abTheta = S1Interval.fromPointPair(Math.atan2(
      a.dotProd(y), a.dotProd(x)), Math.atan2(b.dotProd(y), b.dotProd(x)));

    if (abTheta.contains(theta)) {
      // Check if the intersection point is also in the given "lng" interval.
      S2Point isect = S2Point.add(S2Point.mul(x, cosTheta), S2Point.mul(y,
        sinTheta));
      if (lng.contains(Math.atan2(isect.y, isect.x))) {
        return true;
      }
    }
    if (abTheta.contains(-theta)) {
      // Check if the intersection point is also in the given "lng" interval.
      S2Point intersection = S2Point.sub(S2Point.mul(x, cosTheta), S2Point.mul(y, sinTheta));
      if (lng.contains(Math.atan2(intersection.y, intersection.x))) {
        return true;
      }
    }
    return false;

  }

  @Override
  public String toString() {
    return "[Lo=" + lo() + ", Hi=" + hi() + "]";
  }
}
